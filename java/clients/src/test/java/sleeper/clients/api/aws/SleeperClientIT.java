/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.api.aws;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.clients.api.QueryResultsNotFoundException;
import sleeper.clients.api.SleeperClient;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.util.PollWithRetries;
import sleeper.core.util.ThreadSleepTestHelper;
import sleeper.local.aws.test.LocalAwsTestBase;
import sleeper.parquet.row.ParquetRowWriterFactory;
import sleeper.query.core.model.Query;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.runner.tracker.DynamoDBQueryTracker;
import sleeper.query.runner.tracker.DynamoDBQueryTrackerCreator;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACCOUNT;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class SleeperClientIT extends LocalAwsTestBase {

    Schema schema = createSchemaWithKey("key", new StringType());
    InstanceProperties instanceProperties = createTestInstanceProperties();
    @AutoClose
    SleeperClient sleeperClient;

    @BeforeEach
    void setUp() {
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        instanceProperties.set(QUERY_QUEUE_URL, createSqsQueueGetUrl());
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        sleeperClient = createClient();
    }

    @Test
    void shouldCreateATable() {
        // Given
        String tableName = "table-name";
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, tableName);

        // When
        sleeperClient.addTable(tableProperties, List.of());

        // Then
        assertThat(sleeperClient.doesTableExist(tableName)).isTrue();
    }

    @Test
    void shouldReadQueryResultsFromS3() throws Exception {
        // Given
        String tableName = "query-table";
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, tableName);
        sleeperClient.addTable(tableProperties, List.of());
        createBucket(instanceProperties.get(QUERY_RESULTS_BUCKET));
        Query query = Query.builder()
                .tableName(tableName)
                .queryId("test-query")
                .regions(List.of(new Region(new RangeFactory(schema).createExactRange("key", "A"))))
                .build();
        writeQueryResults(query, "file-1.parquet", List.of(
                new Row(Map.of("key", "A")),
                new Row(Map.of("key", "B"))));
        writeQueryResults(query, "file-2.parquet", List.of(
                new Row(Map.of("key", "C"))));

        // When
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = sleeperClient.getQueryResults(query)) {
            iterator.forEachRemaining(rows::add);
        }

        // Then
        assertThat(rows).containsExactlyInAnyOrder(
                new Row(Map.of("key", "A")),
                new Row(Map.of("key", "B")),
                new Row(Map.of("key", "C")));
    }

    @Test
    void shouldReadQueryResultsFromS3InParallel() throws Exception {
        // Given a client configured to read results files in parallel, and a query with multiple results files
        String tableName = "parallel-query-table";
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, tableName);
        sleeperClient.addTable(tableProperties, List.of());
        createBucket(instanceProperties.get(QUERY_RESULTS_BUCKET));
        Query query = Query.builder()
                .tableName(tableName)
                .queryId("parallel-query")
                .regions(List.of(new Region(new RangeFactory(schema).createExactRange("key", "A"))))
                .build();
        writeQueryResults(query, "file-1.parquet", List.of(
                new Row(Map.of("key", "A")),
                new Row(Map.of("key", "B"))));
        writeQueryResults(query, "file-2.parquet", List.of(
                new Row(Map.of("key", "C"))));
        writeQueryResults(query, "file-3.parquet", List.of(
                new Row(Map.of("key", "D"))));

        // When
        List<Row> rows = new ArrayList<>();
        try (SleeperClient client = createClientBuilder().resultsReadThreads(2).build();
                CloseableIterator<Row> iterator = client.getQueryResults(query)) {
            iterator.forEachRemaining(rows::add);
        }

        // Then
        assertThat(rows).containsExactlyInAnyOrder(
                new Row(Map.of("key", "A")),
                new Row(Map.of("key", "B")),
                new Row(Map.of("key", "C")),
                new Row(Map.of("key", "D")));
    }

    @Test
    void shouldReadEmptyQueryResultsWhenQueryMatchedNoRows() throws Exception {
        // Given
        String tableName = "empty-query-table";
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, tableName);
        sleeperClient.addTable(tableProperties, List.of());
        createBucket(instanceProperties.get(QUERY_RESULTS_BUCKET));
        Query query = Query.builder()
                .tableName(tableName)
                .queryId("empty-query")
                .regions(List.of(new Region(new RangeFactory(schema).createExactRange("key", "A"))))
                .build();
        writeQueryResults(query, "file-1.parquet", List.of());

        // When
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = sleeperClient.getQueryResults(tableName, "empty-query")) {
            iterator.forEachRemaining(rows::add);
        }

        // Then
        assertThat(rows).isEmpty();
    }

    @Test
    void shouldFailReadingQueryResultsWhenNoneWerePublished() {
        // Given
        String tableName = "untracked-query-table";
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, tableName);
        sleeperClient.addTable(tableProperties, List.of());
        createBucket(instanceProperties.get(QUERY_RESULTS_BUCKET));
        Query query = Query.builder()
                .tableName(tableName)
                .queryId("untracked-query")
                .regions(List.of(new Region(new RangeFactory(schema).createExactRange("key", "A"))))
                .build();

        // When / Then
        assertThatThrownBy(() -> sleeperClient.getQueryResults(query))
                .isInstanceOf(QueryResultsNotFoundException.class)
                .hasMessageContaining("untracked-query");
    }

    @Test
    void shouldStreamQueryResultsFromS3AsSubQueriesFinish() throws Exception {
        // Given a table and a query in progress that has published the results of one subquery
        String tableName = "streaming-query-table";
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, tableName);
        sleeperClient.addTable(tableProperties, List.of());
        createBucket(instanceProperties.get(QUERY_RESULTS_BUCKET));
        new DynamoDBQueryTrackerCreator(instanceProperties, dynamoClient).create();
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoClient);
        Query query = Query.builder()
                .tableName(tableName)
                .queryId("streaming-query")
                .regions(List.of(new Region(new RangeFactory(schema).createExactRange("key", "A"))))
                .build();
        queryTracker.queryInProgress(query);
        writeQueryResults(query, "file-1.parquet", List.of(new Row(Map.of("key", "A"))));

        // When the results are streamed, and a second subquery publishes its results and the query completes
        // during the first wait for more results
        List<Row> rows = new ArrayList<>();
        List<Integer> rowsReadBeforeEachWait = new ArrayList<>();
        PollWithRetries poll = PollWithRetries.builder()
                .pollIntervalMillis(10).maxRetries(10)
                .sleepInInterval(ThreadSleepTestHelper.withActionAfterWait(
                        millis -> rowsReadBeforeEachWait.add(rows.size()),
                        () -> {
                            writeQueryResults(query, "file-2.parquet", List.of(new Row(Map.of("key", "B"))));
                            queryTracker.queryCompleted(query, new ResultsOutputInfo(2L, List.of()));
                        }))
                .build();
        try (CloseableIterator<Row> iterator = sleeperClient.submitQueryAndStreamResults(query, poll)) {
            iterator.forEachRemaining(rows::add);
        }

        // Then the first file was read before the query finished
        assertThat(rows).containsExactly(
                new Row(Map.of("key", "A")),
                new Row(Map.of("key", "B")));
        assertThat(rowsReadBeforeEachWait).containsExactly(1);
    }

    @Test
    void shouldStreamQueryResultsFromS3InParallelAsSubQueriesFinish() throws Exception {
        // Given a client configured to read results files in parallel, a table, and a query in progress that has
        // published the results of one subquery
        String tableName = "parallel-streaming-query-table";
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, tableName);
        sleeperClient.addTable(tableProperties, List.of());
        createBucket(instanceProperties.get(QUERY_RESULTS_BUCKET));
        new DynamoDBQueryTrackerCreator(instanceProperties, dynamoClient).create();
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoClient);
        Query query = Query.builder()
                .tableName(tableName)
                .queryId("parallel-streaming-query")
                .regions(List.of(new Region(new RangeFactory(schema).createExactRange("key", "A"))))
                .build();
        queryTracker.queryInProgress(query);
        writeQueryResults(query, "file-1.parquet", List.of(new Row(Map.of("key", "A"))));

        // When the results are streamed, and two more subqueries publish their results and the query completes
        // during the first wait for more results
        List<Row> rows = new ArrayList<>();
        List<Integer> rowsReadBeforeEachWait = new ArrayList<>();
        PollWithRetries poll = PollWithRetries.builder()
                .pollIntervalMillis(10).maxRetries(10)
                .sleepInInterval(ThreadSleepTestHelper.withActionAfterWait(
                        millis -> rowsReadBeforeEachWait.add(rows.size()),
                        () -> {
                            writeQueryResults(query, "file-2.parquet", List.of(new Row(Map.of("key", "B"))));
                            writeQueryResults(query, "file-3.parquet", List.of(new Row(Map.of("key", "C"))));
                            queryTracker.queryCompleted(query, new ResultsOutputInfo(3L, List.of()));
                        }))
                .build();
        try (SleeperClient client = createClientBuilder().resultsReadThreads(2).build();
                CloseableIterator<Row> iterator = client.submitQueryAndStreamResults(query, poll)) {
            iterator.forEachRemaining(rows::add);
        }

        // Then the first file was read before the query finished, and the remaining files were read in parallel
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0)).isEqualTo(new Row(Map.of("key", "A")));
        assertThat(rows.subList(1, 3)).containsExactlyInAnyOrder(
                new Row(Map.of("key", "B")),
                new Row(Map.of("key", "C")));
        assertThat(rowsReadBeforeEachWait).containsExactly(1);
    }

    private void writeQueryResults(Query query, String filename, List<Row> rows) throws IOException {
        Path path = new Path("s3a://" + instanceProperties.get(QUERY_RESULTS_BUCKET)
                + "/query-" + query.getQueryId() + "/" + filename);
        try (ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(path, schema, hadoopConf)) {
            for (Row row : rows) {
                writer.write(row);
            }
        }
    }

    private SleeperClient createClient() {
        return createClientBuilder().build();
    }

    private AwsSleeperClientBuilder createClientBuilder() {
        return SleeperClient.builder()
                .instanceId(instanceProperties.get(ID))
                .awsClients(clients -> clients
                        .accountName(instanceProperties.get(ACCOUNT))
                        .s3Client(s3Client)
                        .dynamoClient(dynamoClient)
                        .sqsClient(sqsClient)
                        .stsClient(stsClient)
                        .awsCredentialsProvider(credentialsProvider))
                .hadoopConf(hadoopConf);
    }
}
