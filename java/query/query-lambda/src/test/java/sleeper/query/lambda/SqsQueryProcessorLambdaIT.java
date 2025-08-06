/*
 * Copyright 2022-2025 Crown Copyright
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

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.ingest.runner.IngestFactory;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.parquet.row.ParquetReaderIterator;
import sleeper.parquet.row.ParquetRowReaderFactory;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryProcessingConfig;
import sleeper.query.core.model.QuerySerDe;
import sleeper.query.core.output.ResultsOutput;
import sleeper.query.core.tracker.QueryStatusReportListener;
import sleeper.query.core.tracker.QueryTrackerStore;
import sleeper.query.core.tracker.TrackedQuery;
import sleeper.query.runner.output.S3ResultsOutput;
import sleeper.query.runner.output.SQSResultsOutput;
import sleeper.query.runner.output.WebSocketOutput;
import sleeper.query.runner.tracker.DynamoDBQueryTracker;
import sleeper.query.runner.tracker.DynamoDBQueryTrackerCreator;
import sleeper.statestore.StateStoreFactory;
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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.query.core.tracker.QueryState.COMPLETED;
import static sleeper.query.core.tracker.QueryState.IN_PROGRESS;
import static sleeper.query.core.tracker.QueryState.QUEUED;

public class SqsQueryProcessorLambdaIT extends LocalStackTestBase {

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
                        .rowCount(0L)
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
                .queryId("abc").rowCount(0L);
        assertThat(queryTracker.getAllQueries())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastUpdateTime", "expiryDate", "subQueryId")
                .containsExactlyInAnyOrder(
                        builder.lastKnownState(IN_PROGRESS).rowCount(0L).build(),
                        builder.lastKnownState(QUEUED).rowCount(0L).build(),
                        builder.lastKnownState(QUEUED).rowCount(0L).build(),
                        builder.lastKnownState(QUEUED).rowCount(0L).build(),
                        builder.lastKnownState(QUEUED).rowCount(0L).build());
        assertThat(queryTracker.getStatus("abc"))
                .usingRecursiveComparison()
                .ignoringFields("lastUpdateTime", "expiryDate")
                .isEqualTo(builder.lastKnownState(IN_PROGRESS).rowCount(0L).build());
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
                .queryId("abc").rowCount(0L);
        assertThat(queryTracker.getAllQueries())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastUpdateTime", "expiryDate", "subQueryId")
                .containsExactlyInAnyOrder(
                        builder.lastKnownState(COMPLETED).rowCount(1461L).build(),
                        builder.lastKnownState(COMPLETED).rowCount(365L).build(),
                        builder.lastKnownState(COMPLETED).rowCount(365L).build(),
                        builder.lastKnownState(COMPLETED).rowCount(365L).build(),
                        builder.lastKnownState(COMPLETED).rowCount(366L).build());
        assertThat(queryTracker.getStatus("abc"))
                .usingRecursiveComparison()
                .ignoringFields("lastUpdateTime", "expiryDate", "subQueryId")
                .isEqualTo(builder.lastKnownState(COMPLETED).rowCount(1461L).build());
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
                .queryId("abc").rowCount(0L);

        assertThat(queryTracker.getStatus("abc"))
                .usingRecursiveComparison()
                .ignoringFields("lastUpdateTime", "expiryDate")
                .isEqualTo(builder.lastKnownState(COMPLETED).rowCount(1461L).build());
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
                .queryId("abc").rowCount(0L);
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
                        .rowCount(10L)
                        .build(),
                        trackedQuery()
                                .queryId("abc")
                                .subQueryId(subQueryId.orElseThrow())
                                .lastKnownState(COMPLETED)
                                .rowCount(10L)
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
        assertThat(status.getRowCount().longValue()).isEqualTo(28);
        assertThat(getNumberOfRowsInFileOutput(instanceProperties, query)).isEqualTo(status.getRowCount().longValue());
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
        resultsPublishConfig.put(ResultsOutput.DESTINATION, S3ResultsOutput.S3);
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
        assertThat(status.getRowCount().longValue()).isEqualTo(28);
        assertThat(getNumberOfRowsInFileOutput(instanceProperties, query)).isEqualTo(status.getRowCount().longValue());
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
        resultsPublishConfig.put(ResultsOutput.DESTINATION, SQSResultsOutput.SQS);
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
        assertThat(status.getRowCount().longValue()).isEqualTo(28);
        assertThat(getNumberOfMessagesInResultsQueue(instanceProperties)).isEqualTo(28);
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
        resultsPublishConfig.put(ResultsOutput.DESTINATION, WebSocketOutput.DESTINATION_NAME);
        resultsPublishConfig.put(WebSocketOutput.ENDPOINT, wireMockServer.baseUrl());
        resultsPublishConfig.put(WebSocketOutput.REGION, "eu-west-1");
        resultsPublishConfig.put(WebSocketOutput.CONNECTION_ID, connectionId);
        resultsPublishConfig.put(WebSocketOutput.MAX_BATCH_SIZE, "1");
        resultsPublishConfig.put(WebSocketOutput.ACCESS_KEY, "accessKey");
        resultsPublishConfig.put(WebSocketOutput.SECRET_KEY, "secretKey");
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
            assertThat(status.getRowCount().longValue()).isEqualTo(28);
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
        resultsPublishConfig.put(ResultsOutput.DESTINATION, WebSocketOutput.DESTINATION_NAME);
        resultsPublishConfig.put(WebSocketOutput.ENDPOINT, wireMockServer.baseUrl());
        resultsPublishConfig.put(WebSocketOutput.REGION, "eu-west-1");
        resultsPublishConfig.put(WebSocketOutput.CONNECTION_ID, connectionId);
        resultsPublishConfig.put(WebSocketOutput.MAX_BATCH_SIZE, "8");
        resultsPublishConfig.put(WebSocketOutput.ACCESS_KEY, "accessKey");
        resultsPublishConfig.put(WebSocketOutput.SECRET_KEY, "secretKey");
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
            assertThat(status.getRowCount().longValue()).isEqualTo(28);
            wireMockServer.verify(4, postRequestedFor(url)); // 4 batches containing max 8 rows each
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
        statusReportDestination.put(QueryStatusReportListener.DESTINATION, WebSocketOutput.DESTINATION_NAME);
        statusReportDestination.put(WebSocketOutput.ENDPOINT, wireMockServer.baseUrl());
        statusReportDestination.put(WebSocketOutput.REGION, "eu-west-1");
        statusReportDestination.put(WebSocketOutput.CONNECTION_ID, connectionId);
        statusReportDestination.put(WebSocketOutput.ACCESS_KEY, "accessKey");
        statusReportDestination.put(WebSocketOutput.SECRET_KEY, "secretKey");
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
            wireMockServer.verify(1, postRequestedFor(url).withRequestBody(equalToJson("""
                    {
                        "queryId": "abc",
                        "message": "subqueries"
                    }
                    """, true, true)));
            wireMockServer.verify(1, postRequestedFor(url).withRequestBody(equalToJson("""
                    {
                        "message": "completed",
                        "rowCount": 28
                    }
                    """, true, true)));
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
        statusReportDestination.put(QueryStatusReportListener.DESTINATION, WebSocketOutput.DESTINATION_NAME);
        statusReportDestination.put(WebSocketOutput.ENDPOINT, wireMockServer.baseUrl());
        statusReportDestination.put(WebSocketOutput.REGION, "eu-west-1");
        statusReportDestination.put(WebSocketOutput.CONNECTION_ID, connectionId);
        statusReportDestination.put(WebSocketOutput.ACCESS_KEY, "accessKey");
        statusReportDestination.put(WebSocketOutput.SECRET_KEY, "secretKey");
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
                            .and(matchingJsonPath("$.rowCount", equalTo("3")))));
            wireMockServer.verify(1, postRequestedFor(url).withRequestBody(
                    matchingJsonPath("$.message", equalTo("completed"))
                            .and(matchingJsonPath("$.rowCount", equalTo("25")))));
        } finally {
            wireMockServer.stop();
        }
    }

    private long getNumberOfMessagesInResultsQueue(InstanceProperties instanceProperties) {
        long count = 0;
        long lastReceiveCount = receiveMessages(instanceProperties.get(QUERY_RESULTS_QUEUE_URL)).count();
        while (lastReceiveCount > 0) {
            count += lastReceiveCount;
            lastReceiveCount = receiveMessages(instanceProperties.get(QUERY_RESULTS_QUEUE_URL)).count();
        }
        return count;
    }

    private long getNumberOfRowsInFileOutput(InstanceProperties instanceProperties, Query query) throws IllegalArgumentException, IOException {
        String fileSystem = instanceProperties.get(FILE_SYSTEM);
        String resultsBucket = instanceProperties.get(QUERY_RESULTS_BUCKET);
        String outputDir = fileSystem + resultsBucket + "/query-" + query.getQueryId();

        long numberOfRowsInOutput = 0;
        RemoteIterator<LocatedFileStatus> outputFiles = FileSystem.get(new Configuration()).listFiles(new Path(outputDir), true);
        while (outputFiles.hasNext()) {
            LocatedFileStatus outputFile = outputFiles.next();
            try (ParquetReader<Row> reader = ParquetRowReaderFactory.parquetRowReaderBuilder(outputFile.getPath(), SCHEMA).build();
                    ParquetReaderIterator it = new ParquetReaderIterator(reader)) {
                while (it.hasNext()) {
                    it.next();
                }
                numberOfRowsInOutput = it.getNumberOfRowsRead();
            }
        }
        return numberOfRowsInOutput;
    }

    private void processQuery(Query query) {
        QuerySerDe querySerDe = new QuerySerDe(S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient));
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
        SQSEvent event = new SQSEvent();
        event.setRecords(receiveMessages(instanceProperties.get(LEAF_PARTITION_QUERY_QUEUE_URL))
                .map(body -> {
                    SQSMessage message = new SQSMessage();
                    message.setBody(body);
                    return message;
                })
                .toList());
        queyLeafPartitionQueryLambda.handleRequest(event, null);
    }

    private void processLeafPartitionQuery() {
        processLeafPartitionQuery(1);
    }

    private void loadData(TableProperties tableProperties, Integer minYear, Integer maxYear) {
        try {
            IngestFactory factory = IngestFactory.builder()
                    .objectFactory(ObjectFactory.noUserJars())
                    .localDir(createTempDirectory(tempDir, null).toString())
                    .stateStoreProvider(StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient))
                    .instanceProperties(instanceProperties)
                    .hadoopConfiguration(hadoopConf)
                    .build();
            factory.ingestFromRowIterator(tableProperties, generateTimeSeriesData(minYear, maxYear).iterator());
        } catch (IOException | IteratorCreationException e) {
            throw new RuntimeException("Failed to Ingest data", e);
        }
    }

    private List<Row> generateTimeSeriesData(Integer minYear, Integer maxYear) {
        LocalDate startDate = LocalDate.of(minYear, 1, 1);
        LocalDate endDate = LocalDate.of(maxYear + 1, 1, 1);
        List<Row> rows = new ArrayList<>();
        for (LocalDate date = startDate; date.isBefore(endDate); date = date.plusDays(1)) {
            Row row = new Row();
            row.put("year", date.getYear());
            row.put("month", date.getMonthValue());
            row.put("day", date.getDayOfMonth());
            row.put("timestamp", Date.from(Timestamp.valueOf(date.atStartOfDay()).toInstant()).getTime());
            row.put("count", (long) date.getYear() * (long) date.getMonthValue() * (long) date.getDayOfMonth());
            HashMap<String, String> map = new HashMap<>();
            map.put(date.getMonth().name(), date.getMonth().name());
            row.put("map", map);
            row.put("list", Lists.newArrayList(date.getEra().toString()));
            row.put("str", date.toString());
            rows.add(row);
        }

        return rows;
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
        S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient).save(tableProperties);

        StateStore stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoClient)
                .getStateStore(tableProperties);
        try {
            update(stateStore).initialise(new PartitionsFromSplitPoints(tableProperties.getSchema(), splitPoints).construct());
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

        instanceProperties.set(QUERY_QUEUE_URL, createSqsQueueGetUrl());
        instanceProperties.set(LEAF_PARTITION_QUERY_QUEUE_URL, createSqsQueueGetUrl());
        instanceProperties.set(QUERY_RESULTS_QUEUE_URL, createSqsQueueGetUrl());
        instanceProperties.set(QUERY_RESULTS_BUCKET, dir + "/query-results");

        createBucket(instanceProperties.get(CONFIG_BUCKET));
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);

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
