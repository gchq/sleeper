/*
 * Copyright 2022-2023 Crown Copyright
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
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.CommonTestConstants;
import sleeper.core.iterator.IteratorException;
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
import sleeper.ingest.IngestFactory;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.query.model.Query;
import sleeper.query.model.QuerySerDe;
import sleeper.query.model.output.ResultsOutputConstants;
import sleeper.query.model.output.S3ResultsOutput;
import sleeper.query.model.output.SQSResultsOutput;
import sleeper.query.model.output.WebSocketResultsOutput;
import sleeper.query.tracker.DynamoDBQueryTracker;
import sleeper.query.tracker.QueryState;
import sleeper.query.tracker.QueryStatusReportListener;
import sleeper.query.tracker.TrackedQuery;
import sleeper.query.tracker.WebSocketQueryStatusReportDestination;
import sleeper.query.tracker.exception.QueryTrackerException;
import sleeper.statestore.InitialiseStateStore;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.table.job.TableCreator;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_TRACKER_TABLE_NAME;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.CommonProperties.ACCOUNT;
import static sleeper.configuration.properties.CommonProperties.FILE_SYSTEM;
import static sleeper.configuration.properties.CommonProperties.ID;
import static sleeper.configuration.properties.IngestProperties.INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.CommonProperties.JARS_BUCKET;
import static sleeper.configuration.properties.CommonProperties.REGION;
import static sleeper.configuration.properties.CommonProperties.SUBNETS;
import static sleeper.configuration.properties.CommonProperties.VPC_ID;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.query.tracker.QueryState.COMPLETED;
import static sleeper.query.tracker.QueryState.IN_PROGRESS;

@Testcontainers
public class SqsQueryProcessorLambdaIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.SQS, LocalStackContainer.Service.DYNAMODB);

    @TempDir
    public java.nio.file.Path tempDir;

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

    @Test
    public void shouldSetStatusOfQueryToCompletedIfLeadingToNoSubQueries() throws ObjectFactoryException, IOException {
        // Given
        String dataDir = createTempDirectory(tempDir, null).toString();
        InstanceProperties instanceProperties = createInstance(dataDir);
        TableProperties timeSeriesTable = createTimeSeriesTable(instanceProperties, 2000, 2020);
        AmazonDynamoDB dynamoClient = createDynamoClient();
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);

        // When
        Range range1 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2000, 2010);
        Range range2 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 0, null);
        Range range3 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 0, null);
        Query query = new Query.Builder(timeSeriesTable.get(TABLE_NAME), "abc", new Region(Arrays.asList(range1, range2, range3))).build();
        this.processQuery(query, instanceProperties);

        // Then
        Map<String, Condition> keyCondition = new HashMap<>();
        keyCondition.put(DynamoDBQueryTracker.QUERY_ID, new Condition()
                .withAttributeValueList(new AttributeValue("abc"))
                .withComparisonOperator(ComparisonOperator.EQ)
        );
        QueryResult response = dynamoClient
                .query(new QueryRequest(instanceProperties.get(QUERY_TRACKER_TABLE_NAME))
                        .withKeyConditions(keyCondition)
                );
        assertThat(response.getCount().intValue()).isOne();
        assertThat(QueryState.valueOf(response.getItems().get(0).get(DynamoDBQueryTracker.LAST_KNOWN_STATE).getS())).isEqualTo(COMPLETED);
    }

    @Test
    public void shouldSplitUpQueryWhenItSpansMultiplePartitions() throws ObjectFactoryException, IOException {
        // Given
        String dataDir = createTempDirectory(tempDir, null).toString();
        InstanceProperties instanceProperties = createInstance(dataDir);
        TableProperties timeSeriesTable = createTimeSeriesTable(instanceProperties, 2000, 2020);
        loadData(instanceProperties, timeSeriesTable, createTempDirectory(tempDir, null).toString(), 2005, 2008);
        AmazonDynamoDB dynamoClient = createDynamoClient();
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);

        // When
        Range range1 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2000, true, 2010, true);
        Range range2 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 0, true, null, true);
        Range range3 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 0, true, null, true);
        Query query = new Query.Builder(timeSeriesTable.get(TABLE_NAME), "abc", new Region(Arrays.asList(range1, range2, range3))).build();
        this.processQuery(query, instanceProperties);

        // Then
        Map<String, Condition> keyCondition = new HashMap<>();
        keyCondition.put(DynamoDBQueryTracker.QUERY_ID, new Condition()
                .withAttributeValueList(new AttributeValue("abc"))
                .withComparisonOperator(ComparisonOperator.EQ)
        );
        QueryResult response = dynamoClient
                .query(new QueryRequest(instanceProperties.get(QUERY_TRACKER_TABLE_NAME))
                        .withKeyConditions(keyCondition)
                );
        //  - Expect 4 plus the original
        assertThat(response.getCount().intValue()).isEqualTo(5);
    }

    @Test
    public void shouldSetStatusOfQueryToIN_PROGRESSWhileSubQueriesAreProcessingAndToCOMPLETEDWhenAllSubQueriesHaveFinished() throws ObjectFactoryException, IOException {
        // Given
        String dataDir = createTempDirectory(tempDir, null).toString();
        InstanceProperties instanceProperties = createInstance(dataDir);
        TableProperties timeSeriesTable = createTimeSeriesTable(instanceProperties, 2000, 2020);
        loadData(instanceProperties, timeSeriesTable, createTempDirectory(tempDir, null).toString(), 2005, 2008);
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoClient = createDynamoClient();
        AmazonSQS sqsClient = createSqsClient();
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);

        // When
        Range range1 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2000, true, 2010, true);
        Range range2 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 0, true, null, true);
        Range range3 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 0, true, null, true);
        Query query = new Query.Builder(timeSeriesTable.get(TABLE_NAME), "abc", new Region(Arrays.asList(range1, range2, range3))).build();
        this.processQuery(query, instanceProperties);

        // Then
        Map<String, Condition> keyCondition = new HashMap<>();
        keyCondition.put(DynamoDBQueryTracker.QUERY_ID, new Condition()
                .withAttributeValueList(new AttributeValue("abc"))
                .withComparisonOperator(ComparisonOperator.EQ)
        );
        keyCondition.put(DynamoDBQueryTracker.SUB_QUERY_ID, new Condition()
                .withAttributeValueList(new AttributeValue(DynamoDBQueryTracker.NON_NESTED_QUERY_PLACEHOLDER))
                .withComparisonOperator(ComparisonOperator.EQ)
        );
        QueryResult response = dynamoClient
                .query(new QueryRequest(instanceProperties.get(QUERY_TRACKER_TABLE_NAME))
                        .withKeyConditions(keyCondition)
                );
        assertThat(response.getCount().intValue()).isOne();
        assertThat(QueryState.valueOf(response.getItems().get(0).get(DynamoDBQueryTracker.LAST_KNOWN_STATE).getS())).isEqualTo(IN_PROGRESS);
        ReceiveMessageRequest request = new ReceiveMessageRequest(instanceProperties.get(QUERY_QUEUE_URL))
                .withMaxNumberOfMessages(1);
        SqsQueryProcessorLambda queryProcessorLambda = new SqsQueryProcessorLambda(s3Client, sqsClient, dynamoClient, instanceProperties.get(CONFIG_BUCKET));
        for (int i = 0; i < 4; i++) {
            ReceiveMessageResult result = sqsClient.receiveMessage(request);
            assertThat(result.getMessages()).hasSize(1);
            SQSEvent event = new SQSEvent();
            SQSMessage sqsMessage = new SQSMessage();
            sqsMessage.setBody(result.getMessages().get(0).getBody());
            event.setRecords(Lists.newArrayList(
                    sqsMessage
            ));
            queryProcessorLambda.handleRequest(event, null);
            response = dynamoClient
                    .query(new QueryRequest(instanceProperties.get(QUERY_TRACKER_TABLE_NAME))
                            .withKeyConditions(keyCondition)
                    );
            assertThat(response.getCount().intValue()).isOne();
            if (i <= 2) {
                assertThat(QueryState.valueOf(response.getItems().get(0).get(DynamoDBQueryTracker.LAST_KNOWN_STATE).getS())).isEqualTo(IN_PROGRESS);
            } else {
                assertThat(QueryState.valueOf(response.getItems().get(0).get(DynamoDBQueryTracker.LAST_KNOWN_STATE).getS())).isEqualTo(COMPLETED);
            }
        }
        ReceiveMessageResult result = sqsClient.receiveMessage(request);
        assertThat(result.getMessages()).isEmpty();
    }

    @Test
    public void shouldSetStatusOfQueryToCOMPLETEDWhenOnlyOneSubQueryIsCreated() throws ObjectFactoryException, IOException {
        // Given
        String dataDir = createTempDirectory(tempDir, null).toString();
        InstanceProperties instanceProperties = createInstance(dataDir);
        TableProperties timeSeriesTable = createTimeSeriesTable(instanceProperties, 2000, 2020);
        loadData(instanceProperties, timeSeriesTable, createTempDirectory(tempDir, null).toString(), 2005, 2008);
        AmazonDynamoDB dynamoClient = createDynamoClient();
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);

        // When
        Range range1 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2006, true, 2006, true);
        Range range2 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 1, true, 2, true);
        Range range3 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 7, true, 3, true);
        Query query = new Query.Builder(timeSeriesTable.get(TABLE_NAME), "abc", new Region(Arrays.asList(range1, range2, range3))).build();
        this.processQuery(query, instanceProperties);

        // Then
        Map<String, Condition> keyCondition = new HashMap<>();
        keyCondition.put(DynamoDBQueryTracker.QUERY_ID, new Condition()
                .withAttributeValueList(new AttributeValue("abc"))
                .withComparisonOperator(ComparisonOperator.EQ)
        );
        keyCondition.put(DynamoDBQueryTracker.SUB_QUERY_ID, new Condition()
                .withAttributeValueList(new AttributeValue(DynamoDBQueryTracker.NON_NESTED_QUERY_PLACEHOLDER))
                .withComparisonOperator(ComparisonOperator.EQ)
        );
        QueryResult response = dynamoClient
                .query(new QueryRequest(instanceProperties.get(QUERY_TRACKER_TABLE_NAME))
                        .withKeyConditions(keyCondition)
                );
        assertThat(response.getCount().intValue()).isOne();
        assertThat(QueryState.valueOf(response.getItems().get(0).get(DynamoDBQueryTracker.LAST_KNOWN_STATE).getS())).isEqualTo(COMPLETED);
    }

    @Test
    public void shouldPublishResultsToS3ByDefault() throws IOException, ObjectFactoryException, QueryTrackerException {
        // Given
        String dataDir = createTempDirectory(tempDir, null).toString();
        InstanceProperties instanceProperties = this.createInstance(dataDir);
        TableProperties timeSeriesTable = this.createTimeSeriesTable(instanceProperties, 2000, 2020);
        this.loadData(instanceProperties, timeSeriesTable, dataDir, 2005, 2008);
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, this.createDynamoClient());

        // When
        Range range11 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2006, true, 2006, true);
        Range range12 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 1, true, 1, true);
        Range range13 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 7, true, 31, true);
        Region region1 = new Region(Arrays.asList(range11, range12, range13));
        Range range21 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), 2006, true, 2006, true);
        Range range22 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(1), 2, true, 2, true);
        Range range23 = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(2), 1, true, 3, true);
        Region region2 = new Region(Arrays.asList(range21, range22, range23));
        Query query = new Query.Builder(timeSeriesTable.get(TABLE_NAME), "abc", Arrays.asList(region1, region2)).build();
        this.processQuery(query, instanceProperties);

        // Then
        TrackedQuery status = queryTracker.getStatus(query.getQueryId());
        assertThat(status.getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(status.getRecordCount().longValue()).isEqualTo(28);
        assertThat(this.getNumberOfRecordsInFileOutput(instanceProperties, query)).isEqualTo(status.getRecordCount().longValue());
    }

    @Test
    public void shouldPublishResultsToS3() throws IOException, ObjectFactoryException, QueryTrackerException {
        // Given
        String dataDir = createTempDirectory(tempDir, null).toString();
        InstanceProperties instanceProperties = this.createInstance(dataDir);
        TableProperties timeSeriesTable = this.createTimeSeriesTable(instanceProperties, 2000, 2020);
        this.loadData(instanceProperties, timeSeriesTable, dataDir, 2005, 2008);
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, this.createDynamoClient());

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
        Query query = new Query.Builder(timeSeriesTable.get(TABLE_NAME), "abc", Arrays.asList(region1, region2))
                .setResultsPublisherConfig(resultsPublishConfig)
                .build();
        this.processQuery(query, instanceProperties);

        // Then
        TrackedQuery status = queryTracker.getStatus(query.getQueryId());
        assertThat(status.getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(status.getRecordCount().longValue()).isEqualTo(28);
        assertThat(this.getNumberOfRecordsInFileOutput(instanceProperties, query)).isEqualTo(status.getRecordCount().longValue());
    }

    @Test
    public void shouldPublishResultsToSQS() throws IOException, ObjectFactoryException, QueryTrackerException {
        // Given
        String dataDir = createTempDirectory(tempDir, null).toString();
        InstanceProperties instanceProperties = this.createInstance(dataDir);
        TableProperties timeSeriesTable = this.createTimeSeriesTable(instanceProperties, 2000, 2020);
        this.loadData(instanceProperties, timeSeriesTable, dataDir, 2005, 2008);
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, this.createDynamoClient());

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
        Query query = new Query.Builder(timeSeriesTable.get(TABLE_NAME), "abc", Arrays.asList(region1, region2))
                .setResultsPublisherConfig(resultsPublishConfig)
                .build();
        this.processQuery(query, instanceProperties);

        // Then
        TrackedQuery status = queryTracker.getStatus(query.getQueryId());
        assertThat(status.getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(status.getRecordCount().longValue()).isEqualTo(28);
        assertThat(this.getNumberOfRecordsInSqsOutput(instanceProperties)).isEqualTo(status.getRecordCount().longValue());
    }

    @Test
    public void shouldPublishResultsToWebSocket() throws ObjectFactoryException, QueryTrackerException, IOException {
        // Given
        String dataDir = createTempDirectory(tempDir, null).toString();
        InstanceProperties instanceProperties = this.createInstance(dataDir);
        TableProperties timeSeriesTable = this.createTimeSeriesTable(instanceProperties, 2000, 2020);
        this.loadData(instanceProperties, timeSeriesTable, dataDir, 2005, 2008);
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, this.createDynamoClient());
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
        Query query = new Query.Builder(timeSeriesTable.get(TABLE_NAME), "abc", Arrays.asList(region1, region2))
                .setResultsPublisherConfig(resultsPublishConfig)
                .build();

        try {
            this.processQuery(query, instanceProperties);

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
    public void shouldPublishResultsToWebSocketInBatches() throws ObjectFactoryException, QueryTrackerException, IOException {
        // Given
        String dataDir = createTempDirectory(tempDir, null).toString();
        InstanceProperties instanceProperties = this.createInstance(dataDir);
        TableProperties timeSeriesTable = this.createTimeSeriesTable(instanceProperties, 2000, 2020);
        this.loadData(instanceProperties, timeSeriesTable, dataDir, 2005, 2008);
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, this.createDynamoClient());
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
        Query query = new Query.Builder(timeSeriesTable.get(TABLE_NAME), "abc", Arrays.asList(region1, region2))
                .setResultsPublisherConfig(resultsPublishConfig)
                .build();

        try {
            this.processQuery(query, instanceProperties);

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
    public void shouldPublishStatusReportsToWebSocket() throws IOException, ObjectFactoryException {
        // Given
        String dataDir = createTempDirectory(tempDir, null).toString();
        InstanceProperties instanceProperties = this.createInstance(dataDir);
        TableProperties timeSeriesTable = this.createTimeSeriesTable(instanceProperties, 2000, 2020);
        this.loadData(instanceProperties, timeSeriesTable, dataDir, 2005, 2008);
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
        Query query = new Query.Builder(timeSeriesTable.get(TABLE_NAME), "abc", Arrays.asList(region1, region2))
                .setStatusReportDestinations(Lists.newArrayList(statusReportDestination))
                .build();

        try {
            this.processQuery(query, instanceProperties);

            // Then
            wireMockServer.verify(1, postRequestedFor(url).withRequestBody(
                    matchingJsonPath("$.queryId", equalTo("abc"))
                            .and(matchingJsonPath("$.message", equalTo("completed")))
                            .and(matchingJsonPath("$.recordCount", equalTo("28")))
            ));
        } finally {
            wireMockServer.stop();
        }
    }

    @Test
    public void shouldPublishMultipleStatusReportsToWebSocketForSubQueries() throws IOException, ObjectFactoryException {
        // Given
        AmazonSQS sqsClient = this.createSqsClient();
        String dataDir = createTempDirectory(tempDir, null).toString();
        InstanceProperties instanceProperties = this.createInstance(dataDir);
        TableProperties timeSeriesTable = this.createTimeSeriesTable(instanceProperties, 2000, 2020);
        this.loadData(instanceProperties, timeSeriesTable, dataDir, 2005, 2008);
        RangeFactory rangeFactory = new RangeFactory(SCHEMA);
        QuerySerDe querySerDe = new QuerySerDe(new TablePropertiesProvider(this.createS3Client(), instanceProperties));
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
        Query query = new Query.Builder(timeSeriesTable.get(TABLE_NAME), "abc", Arrays.asList(region1, region2))
                .setStatusReportDestinations(Lists.newArrayList(statusReportDestination))
                .build();

        try {
            // Process Query
            this.processQuery(query, instanceProperties);

            // Process SubQueries
            ReceiveMessageRequest request = new ReceiveMessageRequest(instanceProperties.get(QUERY_QUEUE_URL)).withMaxNumberOfMessages(1);
            ReceiveMessageResult response;
            do {
                response = sqsClient.receiveMessage(request);
                for (Message message : response.getMessages()) {
                    Query subQuery = querySerDe.fromJson(message.getBody());
                    this.processQuery(subQuery, instanceProperties);
                }
            } while (response != null && response.getMessages().size() > 0);

            // Then
            wireMockServer.verify(1, postRequestedFor(url).withRequestBody(
                    matchingJsonPath("$.queryId", equalTo("abc"))
                            .and(matchingJsonPath("$.message", equalTo("subqueries")))
                            .and(matchingJsonPath("$.queryIds"))
            ));
            wireMockServer.verify(1, postRequestedFor(url).withRequestBody(
                    matchingJsonPath("$.message", equalTo("completed"))
                            .and(matchingJsonPath("$.recordCount", equalTo("3")))
            ));
            wireMockServer.verify(1, postRequestedFor(url).withRequestBody(
                    matchingJsonPath("$.message", equalTo("completed"))
                            .and(matchingJsonPath("$.recordCount", equalTo("25")))
            ));
        } finally {
            wireMockServer.stop();
        }
    }

    private long getNumberOfRecordsInSqsOutput(InstanceProperties instanceProperties) {
        long recordCount = 0;
        AmazonSQS sqsClient = this.createSqsClient();
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

    private void processQuery(Query query, InstanceProperties instanceProperties) throws ObjectFactoryException {
        AmazonS3 s3Client = this.createS3Client();
        AmazonDynamoDB dynamoClient = this.createDynamoClient();
        AmazonSQS sqsClient = this.createSqsClient();

        QuerySerDe querySerDe = new QuerySerDe(new TablePropertiesProvider(s3Client, instanceProperties));
        String jsonQuery = querySerDe.toJson(query);
        SQSEvent event = new SQSEvent();
        SQSMessage sqsMessage = new SQSMessage();
        sqsMessage.setBody(jsonQuery);
        event.setRecords(Lists.newArrayList(sqsMessage));
        SqsQueryProcessorLambda queryProcessorLambda = new SqsQueryProcessorLambda(s3Client, sqsClient, dynamoClient, instanceProperties.get(CONFIG_BUCKET));
        queryProcessorLambda.handleRequest(event, null);
    }

    private void loadData(InstanceProperties instanceProperties, TableProperties tableProperties, String dataDir, Integer minYear, Integer maxYear) {
        AmazonDynamoDB dynamoClient = createDynamoClient();
        AmazonS3 s3Client = createS3Client();
        try {
            Configuration hadoopConfiguration = new Configuration();
            IngestFactory factory = IngestFactory.builder()
                    .objectFactory(ObjectFactory.noUserJars())
                    .localDir(dataDir)
                    .stateStoreProvider(new StateStoreProvider(dynamoClient, instanceProperties, hadoopConfiguration))
                    .instanceProperties(instanceProperties)
                    .hadoopConfiguration(hadoopConfiguration)
                    .build();
            factory.ingestFromRecordIterator(tableProperties, generateTimeSeriesData(minYear, maxYear).iterator());
        } catch (IOException | StateStoreException | IteratorException e) {
            throw new RuntimeException("Failed to Ingest data", e);
        } finally {
            dynamoClient.shutdown();
            s3Client.shutdown();
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

    private TableProperties createTimeSeriesTable(InstanceProperties instanceProperties, Integer minSplitPoint, Integer maxSplitPoint) {
        List<Object> splitPoints = new ArrayList<>();
        for (int i = minSplitPoint; i <= maxSplitPoint; i++) {
            splitPoints.add(i);
        }

        return createTimeSeriesTable(instanceProperties, splitPoints);
    }

    private TableProperties createTimeSeriesTable(InstanceProperties instanceProperties, List<Object> splitPoints) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, UUID.randomUUID().toString());
        tableProperties.setSchema(SCHEMA);

        try {
            String dataDir = createTempDirectory(tempDir, null).toString();
            tableProperties.set(DATA_BUCKET, dataDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoClient = createDynamoClient();
        new TableCreator(s3Client, dynamoClient, instanceProperties).createTable(tableProperties);


        StateStore stateStore = new StateStoreProvider(dynamoClient, instanceProperties).getStateStore(tableProperties);
        try {
            InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(tableProperties, stateStore, splitPoints).run();
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        } finally {
            s3Client.shutdown();
            dynamoClient.shutdown();
        }

        return tableProperties;
    }

    private InstanceProperties createInstance(String dir) {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, UUID.randomUUID().toString());
        instanceProperties.set(JARS_BUCKET, "unused");
        instanceProperties.set(ACCOUNT, "unused");
        instanceProperties.set(REGION, "unused");
        instanceProperties.set(VPC_ID, "unused");
        instanceProperties.set(SUBNETS, "unused");
        instanceProperties.set(JARS_BUCKET, "unused");
        instanceProperties.set(VERSION, "unused");
        instanceProperties.set(FILE_SYSTEM, dir);
        instanceProperties.set(INGEST_PARTITION_FILE_WRITER_TYPE, "direct");

        AmazonDynamoDB dynamoClient = createDynamoClient();
        String trackedQueryTable = UUID.randomUUID().toString();
        instanceProperties.set(QUERY_TRACKER_TABLE_NAME, trackedQueryTable);

        dynamoClient.createTable(new CreateTableRequest(trackedQueryTable, createKeySchema())
                .withAttributeDefinitions(createAttributeDefinitions())
                .withBillingMode(BillingMode.PAY_PER_REQUEST)
        );
        dynamoClient.shutdown();

        AmazonSQS sqsClient = createSqsClient();

        String queryQueue = UUID.randomUUID().toString();
        instanceProperties.set(QUERY_QUEUE_URL, sqsClient.createQueue(queryQueue).getQueueUrl());

        String resultsQueue = UUID.randomUUID().toString();
        instanceProperties.set(QUERY_RESULTS_QUEUE_URL, sqsClient.createQueue(resultsQueue).getQueueUrl());

        sqsClient.shutdown();

        AmazonS3 s3Client = createS3Client();
        String configBucket = UUID.randomUUID().toString();
        s3Client.createBucket(configBucket);
        instanceProperties.set(CONFIG_BUCKET, configBucket);

        String queryResultsBucket = UUID.randomUUID().toString();
        s3Client.createBucket(queryResultsBucket);
        instanceProperties.set(QUERY_RESULTS_BUCKET, queryResultsBucket);


        try {
            instanceProperties.saveToS3(s3Client);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            s3Client.shutdown();
        }

        return instanceProperties;
    }

    private Collection<AttributeDefinition> createAttributeDefinitions() {
        return Lists.newArrayList(
                new AttributeDefinition(DynamoDBQueryTracker.QUERY_ID, ScalarAttributeType.S),
                new AttributeDefinition(DynamoDBQueryTracker.SUB_QUERY_ID, ScalarAttributeType.S)
        );
    }

    private List<KeySchemaElement> createKeySchema() {
        return Lists.newArrayList(
                new KeySchemaElement()
                        .withAttributeName(DynamoDBQueryTracker.QUERY_ID)
                        .withKeyType(KeyType.HASH),
                new KeySchemaElement()
                        .withAttributeName(DynamoDBQueryTracker.SUB_QUERY_ID)
                        .withKeyType(KeyType.RANGE)
        );
    }

    private AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    private AmazonDynamoDB createDynamoClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    private AmazonSQS createSqsClient() {
        return AmazonSQSClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.SQS))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }
}
