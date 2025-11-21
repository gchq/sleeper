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
package sleeper.clients.api;

import org.junit.jupiter.api.Test;

import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.bulkimport.core.configuration.BulkImportPlatform;
import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.clients.query.FakeWebSocketConnection;
import sleeper.clients.query.FakeWebSocketConnection.WebSocketResponse;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequest;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QuerySerDe;
import sleeper.query.core.output.ResultsOutputLocation;
import sleeper.query.core.rowretrieval.QueryExecutor;
import sleeper.query.runner.websocket.QueryWebSocketMessage;
import sleeper.query.runner.websocket.QueryWebSocketMessageSerDe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

class SleeperClientTest {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    Schema schema = createSchemaWithKey("key", new StringType());
    InMemorySleeperInstance instance = new InMemorySleeperInstance(instanceProperties);
    SleeperClient sleeperClient = instance.sleeperClientBuilder().build();
    QuerySerDe querySerDe = new QuerySerDe(schema);
    QueryWebSocketMessageSerDe serDe = QueryWebSocketMessageSerDe.withNoBatchSize(schema);
    Field rowKey = schema.getField("key").orElseThrow();

    @Test
    void validateThatClientCannotBeCreatedWithNulls() {
        assertThatThrownBy(() -> instance.sleeperClientBuilder().instanceProperties(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("instanceProperties");
        assertThatThrownBy(() -> instance.sleeperClientBuilder().tableIndex(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("tableIndex");
        assertThatThrownBy(() -> instance.sleeperClientBuilder().tablePropertiesStore(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("tablePropertiesStore");
        assertThatThrownBy(() -> instance.sleeperClientBuilder().tablePropertiesProvider(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("tablePropertiesProvider");
        assertThatThrownBy(() -> instance.sleeperClientBuilder().stateStoreProvider(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("stateStoreProvider");
        assertThatThrownBy(() -> instance.sleeperClientBuilder().objectFactory(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("objectFactory");
        assertThatThrownBy(() -> instance.sleeperClientBuilder().rowRetrieverProvider(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("rowRetrieverProvider");
        assertThatThrownBy(() -> instance.sleeperClientBuilder().ingestJobSender(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("ingestJobSender");
        assertThatThrownBy(() -> instance.sleeperClientBuilder().bulkImportJobSender(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("bulkImportJobSender");
        assertThatThrownBy(() -> instance.sleeperClientBuilder().ingestBatcherSender(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("ingestBatcherSender");
        assertThatThrownBy(() -> instance.sleeperClientBuilder().bulkExportQuerySender(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("bulkExportQuerySender");
        assertThatThrownBy(() -> instance.sleeperClientBuilder().queryWebSocketSender(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("queryWebSocketSender");
    }

    @Test
    void shouldVerifyThatTableExists() {
        // Given
        String tableName = "table-name";
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, tableName);
        sleeperClient.addTable(tableProperties, List.of());

        // When / Then
        assertThat(sleeperClient.doesTableExist(tableName)).isTrue();
    }

    @Test
    void shouldValidateThatTableDoesNotExist() {
        // When / Then
        assertThat(sleeperClient.doesTableExist("FAKENAME")).isFalse();
    }

    @Test
    void shouldAddTable() {
        // Given
        TableProperties tableProperties = createTableProperties("test-table");
        List<Object> splitPoints = List.of();

        // When
        sleeperClient.addTable(tableProperties, splitPoints);

        // Then
        assertThat(sleeperClient.streamAllTables()).containsExactly(tableProperties.getStatus());
        assertThat(sleeperClient.getStateStore("test-table").getAllPartitions()).isEqualTo(
                new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
    }

    @Test
    void shouldNotAddTableWithInvalidProperties() {
        // Given
        TableProperties tableProperties = createTableProperties("test-table");
        List<Object> splitPoints = List.of();
        tableProperties.set(TABLE_ONLINE, "not-a-boolean");

        // When / Then
        assertThatThrownBy(() -> sleeperClient.addTable(tableProperties, splitPoints))
                .isInstanceOf(SleeperPropertiesInvalidException.class);
        assertThat(sleeperClient.streamAllTables()).isEmpty();
    }

    @Test
    void shouldQueryTable() throws Exception {
        // Given
        addTable("test-table");
        ingest("test-table", List.of(
                new Row(Map.of("key", "A")),
                new Row(Map.of("key", "B"))));
        try (QueryExecutor queryExecutor = sleeperClient.getQueryExecutor("test-table")) {

            // When
            List<Row> rows = new ArrayList<>();
            try (CloseableIterator<Row> iterator = queryExecutor.execute(Query.builder()
                    .tableName("test-table").queryId(UUID.randomUUID().toString())
                    .regions(List.of(new Region(rangeFactory().createExactRange("key", "B"))))
                    .build())) {
                iterator.forEachRemaining(rows::add);
            }

            // Then
            assertThat(rows).containsExactly(
                    new Row(Map.of("key", "B")));
        }
    }

    @Test
    void shouldTestWebSocketExactQuery() throws Exception {
        FakeWebSocketConnection connection = instance.getFakeWebSocketConnection();
        String tableName = "table-name";

        Row expectedRow = new Row(Map.of("key", "123"));
        connection.setFakeResponses(
                message(queryResult("test-query-id", expectedRow)),
                message(completedQuery("test-query-id", 1L)));

        // sleeperClient = instance.sleeperClientBuilder().build();

        addTable(tableName);
        ingest(tableName, List.of(
                new Row(Map.of("key", "123")),
                new Row(Map.of("key", "456")),
                new Row(Map.of("key", "789"))));

        Query query = exactQuery("test-query-id", "123", tableName);

        // When / Then
        assertThat(sleeperClient.queryViaWebSocket(query))
                .isCompletedWithValue(List.of(expectedRow));
        assertThat(connection.getSentMessages())
                .containsExactly(querySerDe.toJson(query));

        assertThat(connection.isConnected()).isFalse();
        assertThat(connection.isClosed()).isTrue();
    }

    @Test
    void shouldTestWebSocketRangeQuery() throws Exception {
        FakeWebSocketConnection connection = instance.getFakeWebSocketConnection();
        String tableName = "table-name";

        Row expectedRow1 = new Row(Map.of("key", "aaa"));
        Row expectedRow2 = new Row(Map.of("key", "bbb"));

        connection.setFakeResponses(
                message(createdSubQueries("test-query-id", "subquery-1", "subquery-2")),
                message(queryResult("subquery-1", expectedRow1)),
                message(completedQuery("subquery-1", 1L)),
                message(queryResult("subquery-2", expectedRow2)),
                message(completedQuery("subquery-2", 1L)));

        // sleeperClient = instance.sleeperClientBuilder().build();

        addTable(tableName);
        ingest(tableName, List.of(
                new Row(Map.of("key", "aaa")),
                new Row(Map.of("key", "bbb")),
                new Row(Map.of("key", "ccc"))));

        Query query = rangeQuery("test-query-id", "aaa", "bbb", tableName);

        // When / Then
        assertThat(sleeperClient.queryViaWebSocket(query))
                .isCompletedWithValue(List.of(expectedRow1, expectedRow2));
        assertThat(connection.isConnected()).isFalse();
        assertThat(connection.isClosed()).isTrue();
        assertThat(connection.getSentMessages())
                .containsExactly(querySerDe.toJson(query));
    }

    @Test
    void shouldIngestParquetFilesFromS3() {
        String tableName = "ingest-table";
        List<String> fileList = List.of("filename1.parquet", "filename2.parquet");

        // When
        String jobId = sleeperClient.ingestFromFiles(tableName, fileList);

        // Then
        assertThat(jobId).isNotBlank();
        assertThat(instance.ingestQueue()).containsExactly(
                IngestJob.builder()
                        .tableName(tableName)
                        .id(jobId)
                        .files(fileList)
                        .build());
    }

    @Test
    void shouldBulkImportParquetFilesFromS3() {
        // Given
        String tableName = "import-table";
        BulkImportPlatform platform = BulkImportPlatform.NonPersistentEMR;
        List<String> fileList = List.of("filename1.parquet", "filename2.parquet");

        // When
        String jobId = sleeperClient.bulkImportFromFiles(tableName, platform, fileList);

        // Then
        assertThat(jobId).isNotBlank();
        assertThat(instance.bulkImportQueues()).isEqualTo(
                Map.of(platform, List.of(
                        BulkImportJob.builder()
                                .id(jobId)
                                .tableName(tableName)
                                .files(fileList)
                                .build())));
    }

    @Test
    void shouldSendParquetFilesToIngestBatcher() {
        String tableName = "ingest-table";
        List<String> fileList = List.of("filename1.parquet", "filename2.parquet");

        // When
        sleeperClient.sendFilesToIngestBatcher(tableName, fileList);

        // Then
        assertThat(instance.ingestBatcherQueue()).containsExactly(
                new IngestBatcherSubmitRequest(tableName, fileList));
    }

    @Test
    void shouldExportFromTableUsingBulkExportQuery() {
        String tableName = "export-table";

        // When
        String exportId = sleeperClient.bulkExport(tableName);

        // Then
        assertThat(exportId).isNotBlank();
        assertThat(instance.bulkExportQueue()).containsExactly(
                BulkExportQuery.builder()
                        .tableName(tableName)
                        .exportId(exportId)
                        .build());
    }

    private TableProperties createTableProperties(String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, tableName);
        return tableProperties;
    }

    private void addTable(String tableName) {
        TableProperties tableProperties = createTableProperties(tableName);
        sleeperClient.addTable(tableProperties, List.of());
    }

    private void ingest(String tableName, List<Row> rows) {
        try (IngestCoordinator<Row> coordinator = instance.ingestByTableName(tableName).createCoordinator()) {
            for (Row row : rows) {
                coordinator.write(row);
            }
        } catch (IteratorCreationException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String createdSubQueries(String queryId, String... subQueryIds) {
        return serDe.toJson(QueryWebSocketMessage.queryWasSplitToSubqueries(queryId, List.of(subQueryIds)));
    }

    private WebSocketResponse message(String message) {
        return client -> client.onMessage(message);
    }

    private Query exactQuery(String queryId, String value, String tableName) {
        return Query.builder()
                .tableName(tableName)
                .queryId(queryId)
                .regions(List.of(new Region(rangeFactory().createExactRange(rowKey, value))))
                .build();
    }

    private Query rangeQuery(String queryId, String min, String max, String tableName) {
        return Query.builder()
                .tableName(tableName)
                .queryId(queryId)
                .regions(List.of(new Region(rangeFactory().createRange(rowKey, min, max))))
                .build();
    }

    private String queryResult(String queryId, Row... rows) {
        return serDe.toJson(QueryWebSocketMessage.rowsBatch(queryId, List.of(rows)));
    }

    private String completedQuery(String queryId, long rowCount) {
        return serDe.toJson(QueryWebSocketMessage.queryCompleted(queryId, rowCount,
                List.of(new ResultsOutputLocation("websocket-endpoint", "test-endpoint"))));
    }

    private RangeFactory rangeFactory() {
        return new RangeFactory(schema);
    }
}
