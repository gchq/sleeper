/*
 * Copyright 2022-2024 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.query.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.IngestFactory;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.query.model.Query;
import sleeper.query.model.QueryProcessingConfig;
import sleeper.query.model.QuerySerDe;
import sleeper.query.output.ResultsOutputConstants;
import sleeper.query.runner.output.S3ResultsOutput;
import sleeper.query.runner.output.SQSResultsOutput;
import sleeper.query.runner.output.WebSocketResultsOutput;
import sleeper.query.runner.tracker.DynamoDBQueryTracker;
import sleeper.query.runner.tracker.DynamoDBQueryTrackerCreator;
import sleeper.query.runner.tracker.WebSocketQueryStatusReportDestination;
import sleeper.query.tracker.QueryStatusReportListener;
import sleeper.query.tracker.QueryTrackerStore;
import sleeper.query.tracker.TrackedQuery;
import sleeper.statestore.StateStoreProvider;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;
import static sleeper.query.tracker.QueryState.COMPLETED;
import static sleeper.query.tracker.QueryState.IN_PROGRESS;
import static sleeper.query.tracker.QueryState.QUEUED;

@Testcontainers
public class SqsQueryProcessorLambdaIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.SQS, LocalStackContainer.Service.DYNAMODB);

    private final AmazonS3 s3Client = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoClient = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    private final AmazonSQS sqsClient = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.SQS, AmazonSQSClientBuilder.standard());
    private final Configuration configuration = getHadoopConfiguration(localStackContainer);
    @TempDir
    public java.nio.file.Path tempDir;
    private InstanceProperties instanceProperties;
    private QueryTrackerStore queryTracker;
    private SqsQueryProcessorLambda queryProcessorLambda;
    private SqsLeafPartitionQueryLambda queyLeafPartitionQueryLambda;

    private static final Schema SCHEMA = Schema.builder()
            .rowKeyFields(
                    new Field("year", new IntType()),
                    new Field("month", new IntType()),
                    new Field("day", new IntType()))
            .sortKeyFields(
                    new Field("timestamp", new LongType()))
            .valueFields(
                    new Field("count", new LongType()),
                    new Field("map", new MapType(new StringType(), new StringType())),
                    new Field("str", new StringType()),
                    new Field("list", new ListType(new StringType())))
            .build();

    @BeforeEach
    void setUp() throws IOException, ObjectFactoryException {
        String dataDir = createTempDirectory(tempDir, null).toString();
        instanceProperties = createInstance(dataDir);
        queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoClient);
        queryProcessorLambda = new SqsQueryProcessorLambda(s3Client, sqsClient, dynamoClient, instanceProperties.get(CONFIG_BUCKET));
        queyLeafPartitionQueryLambda = new SqsLeafPartitionQueryLambda(s3Client, sqsClient, dynamoClient, instanceProperties.get(CONFIG_BUCKET));
    }

    @AfterEach
    void tearDown() {
        s3Client.shutdown();
        dynamoClient.shutdown();
        sqsClient.shutdown();
    }

    @Test
    public void shouldSetStatusOfQueryToCompletedIfLeadingToNoSubQueries() {
        // Given
        TableProperties timeSeriesTable = createTimeSeriesTable(2000, 2020);
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);

        // When
        Range range1 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2000, 2010);
        Range range2 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 0, null);
        Range range3 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 0, null);
        Query query = Query.builder()
                .tableName(timeSeriesTable.get(TABLE_NAME))
                .queryId("abc")
                .regions(List.of(new Region(List.of(range1, range2, range3))))
                .build();
        processQuery(query);

        // Then
        assertThat(queryTracker.getAllQueries())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastUpdateTime", "expiryDate")
                .containsExactly(trackedQuery()
                        .queryId("abc")
                        .lastKnownState(COMPLETED)
                        .recordCount(0L)
                        .build());
    }

    @Test
    public void shouldSplitUpQueryWhenItSpansMultiplePartitions() throws Exception {
        // Given
        TableProperties timeSeriesTable = createTimeSeriesTable(2000, 2020);
        loadData(timeSeriesTable, 2005, 2008);
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);

        // When
        Range range1 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2000, true, 2010, true);
        Range range2 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 0, true, null, true);
        Range range3 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 0, true, null, true);
        Query query = Query.builder()
                .tableName(timeSeriesTable.get(TABLE_NAME))
                .queryId("abc")
                .regions(List.of(new Region(List.of(range1, range2, range3))))
                .build();

        processQuery(query);

        // Then
        TrackedQuery.Builder builder = trackedQuery()
                .queryId("abc").recordCount(0L);
        assertThat(queryTracker.getAllQueries())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastUpdateTime", "expiryDate", "subQueryId")
                .containsExactlyInAnyOrder(
                        builder.lastKnownState(IN_PROGRESS).recordCount(0L).build(),
                        builder.lastKnownState(QUEUED).recordCount(0L).build(),
                        builder.lastKnownState(QUEUED).recordCount(0L).build(),
                        builder.lastKnownState(QUEUED).recordCount(0L).build(),
                        builder.lastKnownState(QUEUED).recordCount(0L).build());
        assertThat(queryTracker.getStatus("abc"))
                .usingRecursiveComparison()
                .ignoringFields("lastUpdateTime", "expiryDate")
                .isEqualTo(builder.lastKnownState(IN_PROGRESS).recordCount(0L).build());
    }

    @Test
    public void shouldSetStatusOfQueryAndSubQueriesToCOMPLETEDWhenAllSubQueriesHaveFinished() throws Exception {
        // Given
        TableProperties timeSeriesTable = createTimeSeriesTable(2000, 2020);
        loadData(timeSeriesTable, 2005, 2008);
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);
        Range range1 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2000, true, 2010, true);
        Range range2 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 0, true, null, true);
        Range range3 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 0, true, null, true);
        Query query = Query.builder()
                .tableName(timeSeriesTable.get(TABLE_NAME))
                .queryId("abc")
                .regions(List.of(new Region(List.of(range1, range2, range3))))
                .build();

        processQuery(query);

        // When
        processLeafPartitionQuery(4);

        // Then
        TrackedQuery.Builder builder = trackedQuery()
                .queryId("abc").recordCount(0L);
        assertThat(queryTracker.getAllQueries())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastUpdateTime", "expiryDate", "subQueryId")
                .containsExactlyInAnyOrder(
                        builder.lastKnownState(COMPLETED).recordCount(1461L).build(),
                        builder.lastKnownState(COMPLETED).recordCount(365L).build(),
                        builder.lastKnownState(COMPLETED).recordCount(365L).build(),
                        builder.lastKnownState(COMPLETED).recordCount(365L).build(),
                        builder.lastKnownState(COMPLETED).recordCount(366L).build());
        assertThat(queryTracker.getStatus("abc"))
                .usingRecursiveComparison()
                .ignoringFields("lastUpdateTime", "expiryDate", "subQueryId")
                .isEqualTo(builder.lastKnownState(COMPLETED).recordCount(1461L).build());
    }

    @Test
    public void shouldSetStatusOfQueryAndSubQueriesToCOMPLETEDWhenAllSubQueriesHaveFinishedForTwoTables() throws Exception {
        // Given
        TableProperties timeSeriesTable = createTimeSeriesTable(2000, 2020);
        loadData(timeSeriesTable, 2005, 2008);
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);
        Range range1 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2000, true, 2010, true);
        Range range2 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 0, true, null, true);
        Range range3 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 0, true, null, true);
        Query query = Query.builder()
                .tableName(timeSeriesTable.get(TABLE_NAME))
                .queryId("abc")
                .regions(List.of(new Region(List.of(range1, range2, range3))))
                .build();

        processQuery(query);

        // When
        processLeafPartitionQuery(4);

        // Then
        TrackedQuery.Builder builder = trackedQuery()
                .queryId("abc").recordCount(0L);

        assertThat(queryTracker.getStatus("abc"))
                .usingRecursiveComparison()
                .ignoringFields("lastUpdateTime", "expiryDate")
                .isEqualTo(builder.lastKnownState(COMPLETED).recordCount(1461L).build());
        // Given
        timeSeriesTable = createTimeSeriesTable(2000, 2020);
        query = Query.builder()
                .tableName(timeSeriesTable.get(TABLE_NAME))
                .queryId("abc")
                .regions(List.of(new Region(List.of(range1, range2, range3))))
                .build();

        processQuery(query);

        // When
        processLeafPartitionQuery(4);

        // Then
        builder = trackedQuery()
                .queryId("abc").recordCount(0L);
        assertThat(queryTracker.getStatus("abc"))
                .usingRecursiveComparison()
                .ignoringFields("lastUpdateTime", "expiryDate")
                .isEqualTo(builder.lastKnownState(COMPLETED).build());
    }

    @Test
    public void shouldSetStatusOfQueryToCOMPLETEDWhenOnlyOneSubQueryIsCreated() {
        // Given
        TableProperties timeSeriesTable = createTimeSeriesTable(2000, 2020);
        loadData(timeSeriesTable, 2005, 2008);
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);

        // When
        Range range1 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2006, true, 2006, true);
        Range range2 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 1, true, 2, true);
        Range range3 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 3, true, 7, true);
        Query query = Query.builder()
                .tableName(timeSeriesTable.get(TABLE_NAME))
                .queryId("abc")
                .regions(List.of(new Region(List.of(range1, range2, range3))))
                .build();

        processQuery(query);
        processLeafPartitionQuery();

        // Then
        Optional<String> subQueryId = queryTracker.getAllQueries().stream().map(TrackedQuery::getSubQueryId).filter(s -> !s.equals("-")).findFirst();
        assertThat(queryTracker.getAllQueries())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastUpdateTime", "expiryDate")
                .containsExactlyInAnyOrder(trackedQuery()
                        .queryId("abc")
                        .subQueryId("-")
                        .lastKnownState(COMPLETED)
                        .recordCount(10L)
                        .build(),
                        trackedQuery()
                                .queryId("abc")
                                .subQueryId(subQueryId.orElseThrow())
                                .lastKnownState(COMPLETED)
                                .recordCount(10L)
                                .build());
    }

    @Test
    public void shouldPublishResultsToS3ByDefault() throws Exception {
        // Given
        TableProperties timeSeriesTable = createTimeSeriesTable(2000, 2020);
        loadData(timeSeriesTable, 2005, 2008);
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);

        // When
        Range range11 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2006, true, 2006, true);
        Range range12 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 1, true, 1, true);
        Range range13 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 7, true, 31, true);
        Region region1 = new Region(Arrays.asList(range11, range12, range13));
        Range range21 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2006, true, 2006, true);
        Range range22 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 2, true, 2, true);
        Range range23 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 1, true, 3, true);
        Region region2 = new Region(Arrays.asList(range21, range22, range23));
        Query query = Query.builder()
                .tableName(timeSeriesTable.get(TABLE_NAME))
                .queryId("abc")
                .regions(List.of(region1, region2))
                .build();

        processQuery(query);
        processLeafPartitionQuery();

        // Then
        TrackedQuery status = queryTracker.getStatus(query.getQueryId());
        assertThat(status.getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(status.getRecordCount().longValue()).isEqualTo(28);
        assertThat(this.getNumberOfRecordsInFileOutput(instanceProperties, query)).isEqualTo(status.getRecordCount().longValue());
    }

    @Test
    public void shouldPublishResultsToS3() throws Exception {
        // Given
        TableProperties timeSeriesTable = this.createTimeSeriesTable(2000, 2020);
        loadData(timeSeriesTable, 2005, 2008);
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);

        // When
        Range range11 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2006, true, 2006, true);
        Range range12 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 1, true, 1, true);
        Range range13 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 7, true, 31, true);
        Region region1 = new Region(Arrays.asList(range11, range12, range13));
        Range range21 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2006, true, 2006, true);
        Range range22 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 2, true, 2, true);
        Range range23 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 1, true, 3, true);
        Region region2 = new Region(Arrays.asList(range21, range22, range23));
        Map<String, String> resultsPublishConfig = new HashMap<>();
        resultsPublishConfig.put(ResultsOutputConstants.DESTINATION, S3ResultsOutput.S3);
        resultsPublishConfig.put(S3ResultsOutput.S3_BUCKET, instanceProperties.get(QUERY_RESULTS_BUCKET));
        Query query = Query.builder()
                .tableName(timeSeriesTable.get(TABLE_NAME))
                .queryId("abc")
                .regions(List.of(region1, region2))
                .processingConfig(QueryProcessingConfig.builder()
                        .resultsPublisherConfig(resultsPublishConfig)
                        .build())
                .build();

        processQuery(query);
        processLeafPartitionQuery();

        // Then
        TrackedQuery status = queryTracker.getStatus(query.getQueryId());
        assertThat(status.getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(status.getRecordCount().longValue()).isEqualTo(28);
        assertThat(this.getNumberOfRecordsInFileOutput(instanceProperties, query)).isEqualTo(status.getRecordCount().longValue());
    }

    @Test
    public void shouldPublishResultsToSQS() throws Exception {
        // Given
        TableProperties timeSeriesTable = createTimeSeriesTable(2000, 2020);
        loadData(timeSeriesTable, 2005, 2008);
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);

        // When
        Range range11 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2006, true, 2006, true);
        Range range12 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 1, true, 1, true);
        Range range13 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 7, true, 31, true);
        Region region1 = new Region(Arrays.asList(range11, range12, range13));
        Range range21 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2006, true, 2006, true);
        Range range22 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 2, true, 2, true);
        Range range23 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 1, true, 3, true);
        Region region2 = new Region(Arrays.asList(range21, range22, range23));
        Map<String, String> resultsPublishConfig = new HashMap<>();
        resultsPublishConfig.put(ResultsOutputConstants.DESTINATION, SQSResultsOutput.SQS);
        resultsPublishConfig.put(SQSResultsOutput.SQS_RESULTS_URL, instanceProperties.get(QUERY_RESULTS_QUEUE_URL));
        resultsPublishConfig.put(SQSResultsOutput.BATCH_SIZE, "1");
        Query query = Query.builder()
                .tableName(timeSeriesTable.get(TABLE_NAME))
                .queryId("abc")
                .regions(List.of(region1, region2))
                .processingConfig(QueryProcessingConfig.builder()
                        .resultsPublisherConfig(resultsPublishConfig)
                        .build())
                .build();

        processQuery(query);
        processLeafPartitionQuery();

        // Then
        TrackedQuery status = queryTracker.getStatus(query.getQueryId());
        assertThat(status.getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(status.getRecordCount().longValue()).isEqualTo(28);
        assertThat(this.getNumberOfRecordsInSqsOutput(instanceProperties)).isEqualTo(28);
    }

    @Test
    public void shouldPublishResultsToWebSocket() throws Exception {
        // Given
        TableProperties timeSeriesTable = createTimeSeriesTable(2000, 2020);
        loadData(timeSeriesTable, 2005, 2008);
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);
        String connectionId = "connection1";
        WireMockServer wireMockServer = new WireMockServer();
        UrlPattern url = urlEqualTo("/@connections/" + connectionId);
        wireMockServer.stubFor(post(url).willReturn(aResponse().withStatus(200)));
        wireMockServer.start();

        // When
        Range range11 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2006, true, 2006, true);
        Range range12 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 1, true, 1, true);
        Range range13 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 7, true, 31, true);
        Region region1 = new Region(Arrays.asList(range11, range12, range13));
        Range range21 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2006, true, 2006, true);
        Range range22 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 2, true, 2, true);
        Range range23 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 1, true, 3, true);
        Region region2 = new Region(Arrays.asList(range21, range22, range23));
        Map<String, String> resultsPublishConfig = new HashMap<>();
        resultsPublishConfig.put(ResultsOutputConstants.DESTINATION, WebSocketResultsOutput.DESTINATION_NAME);
        resultsPublishConfig.put(WebSocketResultsOutput.ENDPOINT, wireMockServer.baseUrl());
        resultsPublishConfig.put(WebSocketResultsOutput.REGION, "eu-west-1");
        resultsPublishConfig.put(WebSocketResultsOutput.CONNECTION_ID, connectionId);
        resultsPublishConfig.put(WebSocketResultsOutput.MAX_BATCH_SIZE, "1");
        resultsPublishConfig.put(WebSocketResultsOutput.ACCESS_KEY, "accessKey");
        resultsPublishConfig.put(WebSocketResultsOutput.SECRET_KEY, "secretKey");
        Query query = Query.builder()
                .tableName(timeSeriesTable.get(TABLE_NAME))
                .queryId("abc")
                .regions(List.of(region1, region2))
                .processingConfig(QueryProcessingConfig.builder()
                        .resultsPublisherConfig(resultsPublishConfig)
                        .build())
                .build();

        try {
            processQuery(query);
            processLeafPartitionQuery();

            // Then
            TrackedQuery status = queryTracker.getStatus(query.getQueryId());
            assertThat(status.getLastKnownState()).isEqualTo(COMPLETED);
            assertThat(status.getRecordCount().longValue()).isEqualTo(28);
            wireMockServer.verify(28, postRequestedFor(url));
            wireMockServer.verify(1, postRequestedFor(url).withRequestBody(containing("\"day\":2,")));
        } finally {
            wireMockServer.stop();
        }
    }

    @Test
    public void shouldPublishResultsToWebSocketInBatches() throws Exception {
        // Given
        TableProperties timeSeriesTable = createTimeSeriesTable(2000, 2020);
        loadData(timeSeriesTable, 2005, 2008);
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);
        String connectionId = "connection1";
        WireMockServer wireMockServer = new WireMockServer();
        UrlPattern url = urlEqualTo("/@connections/" + connectionId);
        wireMockServer.stubFor(post(url).willReturn(aResponse().withStatus(200)));
        wireMockServer.start();

        // When
        Range range11 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2006, true, 2006, true);
        Range range12 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 1, true, 1, true);
        Range range13 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 7, true, 31, true);
        Region region1 = new Region(Arrays.asList(range11, range12, range13));
        Range range21 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2006, true, 2006, true);
        Range range22 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 2, true, 2, true);
        Range range23 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 1, true, 3, true);
        Region region2 = new Region(Arrays.asList(range21, range22, range23));
        Map<String, String> resultsPublishConfig = new HashMap<>();
        resultsPublishConfig.put(ResultsOutputConstants.DESTINATION, WebSocketResultsOutput.DESTINATION_NAME);
        resultsPublishConfig.put(WebSocketResultsOutput.ENDPOINT, wireMockServer.baseUrl());
        resultsPublishConfig.put(WebSocketResultsOutput.REGION, "eu-west-1");
        resultsPublishConfig.put(WebSocketResultsOutput.CONNECTION_ID, connectionId);
        resultsPublishConfig.put(WebSocketResultsOutput.MAX_BATCH_SIZE, "8");
        resultsPublishConfig.put(WebSocketResultsOutput.ACCESS_KEY, "accessKey");
        resultsPublishConfig.put(WebSocketResultsOutput.SECRET_KEY, "secretKey");
        Query query = Query.builder()
                .tableName(timeSeriesTable.get(TABLE_NAME))
                .queryId("abc")
                .regions(List.of(region1, region2))
                .processingConfig(QueryProcessingConfig.builder()
                        .resultsPublisherConfig(resultsPublishConfig)
                        .build())
                .build();

        try {
            processQuery(query);
            processLeafPartitionQuery();

            // Then
            TrackedQuery status = queryTracker.getStatus(query.getQueryId());
            assertThat(status.getLastKnownState()).isEqualTo(COMPLETED);
            assertThat(status.getRecordCount().longValue()).isEqualTo(28);
            wireMockServer.verify(4, postRequestedFor(url)); // 4 batches containing max 8 records each
        } finally {
            wireMockServer.stop();
        }
    }

    @Test
    public void shouldPublishStatusReportsToWebSocket() {
        // Given
        TableProperties timeSeriesTable = this.createTimeSeriesTable(2000, 2020);
        loadData(timeSeriesTable, 2005, 2008);
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);
        String connectionId = "connection1";
        WireMockServer wireMockServer = new WireMockServer();
        UrlPattern url = urlEqualTo("/@connections/" + connectionId);
        wireMockServer.stubFor(post(url).willReturn(aResponse().withStatus(200)));
        wireMockServer.start();

        // When
        Range range11 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2006, true, 2006, true);
        Range range12 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 1, true, 1, true);
        Range range13 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 7, true, 31, true);
        Region region1 = new Region(Arrays.asList(range11, range12, range13));
        Range range21 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2006, true, 2006, true);
        Range range22 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 2, true, 2, true);
        Range range23 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 1, true, 3, true);
        Region region2 = new Region(Arrays.asList(range21, range22, range23));
        Map<String, String> statusReportDestination = new HashMap<>();
        statusReportDestination.put(QueryStatusReportListener.DESTINATION, WebSocketQueryStatusReportDestination.DESTINATION_NAME);
        statusReportDestination.put(WebSocketResultsOutput.ENDPOINT, wireMockServer.baseUrl());
        statusReportDestination.put(WebSocketResultsOutput.REGION, "eu-west-1");
        statusReportDestination.put(WebSocketResultsOutput.CONNECTION_ID, connectionId);
        statusReportDestination.put(WebSocketResultsOutput.ACCESS_KEY, "accessKey");
        statusReportDestination.put(WebSocketResultsOutput.SECRET_KEY, "secretKey");
        Query query = Query.builder()
                .tableName(timeSeriesTable.get(TABLE_NAME))
                .queryId("abc")
                .regions(List.of(region1, region2))
                .processingConfig(QueryProcessingConfig.builder()
                        .statusReportDestinations(List.of(statusReportDestination))
                        .build())
                .build();
        try {
            processQuery(query);
            processLeafPartitionQuery();

            // Then
            wireMockServer.verify(2, postRequestedFor(url));
            wireMockServer.verify(1, postRequestedFor(url).withRequestBody(
                    matchingJsonPath("$.queryId", equalTo("abc"))));
            wireMockServer.verify(1, postRequestedFor(url).withRequestBody(
                    matchingJsonPath("$.message", equalTo("completed"))
                            .and(matchingJsonPath("$.recordCount", equalTo("28")))));
        } finally {
            wireMockServer.stop();
        }
    }

    @Test
    public void shouldPublishMultipleStatusReportsToWebSocketForSubQueries() {
        // Given
        TableProperties timeSeriesTable = createTimeSeriesTable(2000, 2020);
        loadData(timeSeriesTable, 2005, 2008);
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);
        String connectionId = "connection1";
        WireMockServer wireMockServer = new WireMockServer();
        UrlPattern url = urlEqualTo("/@connections/" + connectionId);
        wireMockServer.stubFor(post(url).willReturn(aResponse().withStatus(200)));
        wireMockServer.start();

        // When
        Range range11 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2006, true, 2006, true);
        Range range12 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 1, true, 1, true);
        Range range13 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 7, true, 31, true);
        Region region1 = new Region(Arrays.asList(range11, range12, range13));
        Range range21 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2007, true, 2007, true);
        Range range22 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 2, true, 2, true);
        Range range23 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 1, true, 3, true);
        Region region2 = new Region(Arrays.asList(range21, range22, range23));
        Map<String, String> statusReportDestination = new HashMap<>();
        statusReportDestination.put(QueryStatusReportListener.DESTINATION, WebSocketQueryStatusReportDestination.DESTINATION_NAME);
        statusReportDestination.put(WebSocketResultsOutput.ENDPOINT, wireMockServer.baseUrl());
        statusReportDestination.put(WebSocketResultsOutput.REGION, "eu-west-1");
        statusReportDestination.put(WebSocketResultsOutput.CONNECTION_ID, connectionId);
        statusReportDestination.put(WebSocketResultsOutput.ACCESS_KEY, "accessKey");
        statusReportDestination.put(WebSocketResultsOutput.SECRET_KEY, "secretKey");
        Query query = Query.builder()
                .tableName(timeSeriesTable.get(TABLE_NAME))
                .queryId("abc")
                .regions(List.of(region1, region2))
                .processingConfig(QueryProcessingConfig.builder()
                        .statusReportDestinations(List.of(statusReportDestination))
                        .build())
                .build();

        try {
            // Process Query
            processQuery(query);
            processLeafPartitionQuery(2);

            // Then
            wireMockServer.verify(3, postRequestedFor(url));
            wireMockServer.verify(1, postRequestedFor(url).withRequestBody(
                    matchingJsonPath("$.queryId", equalTo("abc"))
                            .and(matchingJsonPath("$.message", equalTo("subqueries")))
                            .and(matchingJsonPath("$.queryIds"))));
            wireMockServer.verify(1, postRequestedFor(url).withRequestBody(
                    matchingJsonPath("$.message", equalTo("completed"))
                            .and(matchingJsonPath("$.recordCount", equalTo("3")))));
            wireMockServer.verify(1, postRequestedFor(url).withRequestBody(
                    matchingJsonPath("$.message", equalTo("completed"))
                            .and(matchingJsonPath("$.recordCount", equalTo("25")))));
        } finally {
            wireMockServer.stop();
        }
    }

    private long getNumberOfRecordsInSqsOutput(InstanceProperties instanceProperties) {
        long recordCount = 0;
        ReceiveMessageRequest request = new ReceiveMessageRequest(instanceProperties.get(QUERY_RESULTS_QUEUE_URL))
                .withMaxNumberOfMessages(1);
        int lastReceiveCount = -1;
        while (lastReceiveCount != 0) {
            ReceiveMessageResult response = sqsClient.receiveMessage(request);
            lastReceiveCount = response.getMessages().size();
            recordCount += lastReceiveCount;
        }
        return recordCount;
    }

    private long getNumberOfRecordsInFileOutput(InstanceProperties instanceProperties, Query query) throws IllegalArgumentException, IOException {
        String fileSystem = instanceProperties.get(FILE_SYSTEM);
        String resultsBucket = instanceProperties.get(QUERY_RESULTS_BUCKET);
        String outputDir = fileSystem + resultsBucket + "/query-" + query.getQueryId();

        long numberOfRecordsInOutput = 0;
        RemoteIterator<LocatedFileStatus> outputFiles = FileSystem.get(new Configuration()).listFiles(new Path(outputDir), true);
        while (outputFiles.hasNext()) {
            LocatedFileStatus outputFile = outputFiles.next();
            try (ParquetReader<Record> reader = new ParquetRecordReader.Builder(outputFile.getPath(), SCHEMA).build()) {
                ParquetReaderIterator it = new ParquetReaderIterator(reader);
                while (it.hasNext()) {
                    it.next();
                }
                numberOfRecordsInOutput = it.getNumberOfRecordsRead();
            }
        }
        return numberOfRecordsInOutput;
    }

    private void processQuery(Query query) {
        QuerySerDe querySerDe = new QuerySerDe(new TablePropertiesProvider(instanceProperties, s3Client, dynamoClient));
        String jsonQuery = querySerDe.toJson(query);
        processQuery(jsonQuery);
    }

    private void processQuery(String jsonQuery) {
        SQSEvent event = new SQSEvent();
        SQSMessage sqsMessage = new SQSMessage();
        sqsMessage.setBody(jsonQuery);
        event.setRecords(Lists.newArrayList(sqsMessage));
        queryProcessorLambda.handleRequest(event, null);
    }

    private void processLeafPartitionQuery(int maxMessages) {
        List<SQSMessage> leafPartitionQueries = new ArrayList<>();
        sqsClient.receiveMessage(new ReceiveMessageRequest()
                .withQueueUrl(instanceProperties.get(LEAF_PARTITION_QUERY_QUEUE_URL))
                .withMaxNumberOfMessages(maxMessages)).getMessages().forEach(message -> {
                    SQSMessage leafMessage = new SQSMessage();
                    leafMessage.setBody(message.getBody());
                    leafPartitionQueries.add(leafMessage);
                });

        SQSEvent leafEvent = new SQSEvent();
        leafEvent.setRecords(Lists.newArrayList(leafPartitionQueries));
        queyLeafPartitionQueryLambda.handleRequest(leafEvent, null);
    }

    private void processLeafPartitionQuery() {
        processLeafPartitionQuery(1);
    }

    private void loadData(TableProperties tableProperties, Integer minYear, Integer maxYear) {
        try {
            IngestFactory factory = IngestFactory.builder()
                    .objectFactory(ObjectFactory.noUserJars())
                    .localDir(createTempDirectory(tempDir, null).toString())
                    .stateStoreProvider(new StateStoreProvider(instanceProperties, s3Client, dynamoClient, configuration))
                    .instanceProperties(instanceProperties)
                    .hadoopConfiguration(configuration)
                    .build();
            factory.ingestFromRecordIterator(tableProperties, generateTimeSeriesData(minYear, maxYear).iterator());
        } catch (IOException | StateStoreException | IteratorCreationException e) {
            throw new RuntimeException("Failed to Ingest data", e);
        }
    }

    private List<Record> generateTimeSeriesData(Integer minYear, Integer maxYear) {
        LocalDate startDate = LocalDate.of(minYear, 1, 1);
        LocalDate endDate = LocalDate.of(maxYear + 1, 1, 1);
        List<Record> records = new ArrayList<>();
        for (LocalDate date = startDate; date.isBefore(endDate); date = date.plusDays(1)) {
            Record record = new Record();
            record.put("year", date.getYear());
            record.put("month", date.getMonthValue());
            record.put("day", date.getDayOfMonth());
            record.put("timestamp", Date.from(Timestamp.valueOf(date.atStartOfDay()).toInstant()).getTime());
            record.put("count", (long) date.getYear() * (long) date.getMonthValue() * (long) date.getDayOfMonth());
            HashMap<String, String> map = new HashMap<>();
            map.put(date.getMonth().name(), date.getMonth().name());
            record.put("map", map);
            record.put("list", Lists.newArrayList(date.getEra().toString()));
            record.put("str", date.toString());
            records.add(record);
        }

        return records;
    }

    private TableProperties createTimeSeriesTable(Integer minSplitPoint, Integer maxSplitPoint) {
        List<Object> splitPoints = new ArrayList<>();
        for (int i = minSplitPoint; i <= maxSplitPoint; i++) {
            splitPoints.add(i);
        }

        return createTimeSeriesTable(splitPoints);
    }

    private TableProperties createTimeSeriesTable(List<Object> splitPoints) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, SCHEMA);
        S3TableProperties.getStore(instanceProperties, s3Client, dynamoClient).save(tableProperties);

        StateStore stateStore = new StateStoreProvider(instanceProperties, s3Client, dynamoClient, configuration)
                .getStateStore(tableProperties);
        try {
            stateStore.initialise(new PartitionsFromSplitPoints(tableProperties.getSchema(), splitPoints).construct());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }

        return tableProperties;
    }

    private InstanceProperties createInstance(String dir) {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, dir);
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");

        new DynamoDBQueryTrackerCreator(instanceProperties, dynamoClient).create();

        instanceProperties.set(QUERY_QUEUE_URL, sqsClient.createQueue(UUID.randomUUID().toString()).getQueueUrl());
        instanceProperties.set(LEAF_PARTITION_QUERY_QUEUE_URL, sqsClient.createQueue(UUID.randomUUID().toString()).getQueueUrl());
        instanceProperties.set(QUERY_RESULTS_QUEUE_URL, sqsClient.createQueue(UUID.randomUUID().toString()).getQueueUrl());
        instanceProperties.set(QUERY_RESULTS_BUCKET, dir + "/query-results");

        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));
        instanceProperties.saveToS3(s3Client);

        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();

        return instanceProperties;
    }

    private TrackedQuery.Builder trackedQuery() {
        return TrackedQuery.builder()
                .lastUpdateTime(Instant.EPOCH)
                .expiryDate(Instant.EPOCH);
    }
}
