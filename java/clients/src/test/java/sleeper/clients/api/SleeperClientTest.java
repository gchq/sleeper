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

import sleeper.bulkimport.core.configuration.BulkImportPlatform;
import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.record.testutils.InMemoryRecordStore;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequest;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.ingest.runner.testutils.InMemoryIngest;
import sleeper.ingest.runner.testutils.InMemorySketchesStore;
import sleeper.query.core.model.Query;
import sleeper.query.core.recordretrieval.InMemoryLeafPartitionRecordRetriever;
import sleeper.query.core.recordretrieval.QueryExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
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
    TableIndex tableIndex = new InMemoryTableIndex();
    TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStoreReturningExactInstance(tableIndex);
    Schema schema = createSchemaWithKey("key", new StringType());
    InMemoryRecordStore dataStore = new InMemoryRecordStore();
    InMemorySketchesStore sketchesStore = new InMemorySketchesStore();
    Queue<IngestJob> ingestQueue = new LinkedList<>();
    Map<BulkImportPlatform, Queue<BulkImportJob>> bulkImportQueues = new HashMap<>();
    Queue<IngestBatcherSubmitRequest> ingestBatcherQueue = new LinkedList<>();
    SleeperClient sleeperClient = createSleeperBuilder().build();

    private SleeperClient.Builder createSleeperBuilder() {
        return new SleeperClient.Builder()
                .instanceProperties(instanceProperties)
                .tableIndex(tableIndex)
                .tablePropertiesStore(tablePropertiesStore)
                .tablePropertiesProvider(new TablePropertiesProvider(instanceProperties, tablePropertiesStore))
                .stateStoreProvider(InMemoryTransactionLogStateStore.createProvider(instanceProperties, new InMemoryTransactionLogsPerTable()))
                .recordRetrieverProvider(new InMemoryLeafPartitionRecordRetriever(dataStore))
                .ingestJobSender(ingestQueue::add)
                .bulkImportJobSender(bulkImportSender())
                .ingestBatcherSender(ingestBatcherQueue::add);
    }

    @Test
    void validateThatClientCannotBeCreatedWithNulls() {
        assertThatThrownBy(() -> createSleeperBuilder().instanceProperties(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("instanceProperties");
        assertThatThrownBy(() -> createSleeperBuilder().tableIndex(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("tableIndex");
        assertThatThrownBy(() -> createSleeperBuilder().tablePropertiesStore(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("tablePropertiesStore");
        assertThatThrownBy(() -> createSleeperBuilder().tablePropertiesProvider(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("tablePropertiesProvider");
        assertThatThrownBy(() -> createSleeperBuilder().stateStoreProvider(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("stateStoreProvider");
        assertThatThrownBy(() -> createSleeperBuilder().objectFactory(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("objectFactory");
        assertThatThrownBy(() -> createSleeperBuilder().recordRetrieverProvider(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("recordRetrieverProvider");
        assertThatThrownBy(() -> createSleeperBuilder().ingestJobSender(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("ingestJobSender");
        assertThatThrownBy(() -> createSleeperBuilder().bulkImportJobSender(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("bulkImportJobSender");
        assertThatThrownBy(() -> createSleeperBuilder().ingestBatcherSender(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("ingestBatcherSender");
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
                new Record(Map.of("key", "A")),
                new Record(Map.of("key", "B"))));
        QueryExecutor queryExecutor = sleeperClient.getQueryExecutor("test-table");

        // When
        List<Record> records = new ArrayList<>();
        try (CloseableIterator<Record> iterator = queryExecutor.execute(Query.builder()
                .tableName("test-table").queryId(UUID.randomUUID().toString())
                .regions(List.of(new Region(rangeFactory().createExactRange("key", "B"))))
                .build())) {
            iterator.forEachRemaining(records::add);
        }

        // Then
        assertThat(records).containsExactly(
                new Record(Map.of("key", "B")));
    }

    @Test
    void shouldIngestParquetFilesFromS3() {
        String tableName = "ingest-table";
        List<String> fileList = List.of("filename1.parquet", "filename2.parquet");

        // When
        String jobId = sleeperClient.ingestParquetFilesFromS3(tableName, fileList);

        // Then
        assertThat(jobId).isNotBlank();
        assertThat(ingestQueue).containsExactly(
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
        String jobId = sleeperClient.bulkImportParquetFilesFromS3(tableName, platform, fileList);

        // Then
        assertThat(jobId).isNotBlank();
        assertThat(bulkImportQueues).isEqualTo(
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
        assertThat(ingestBatcherQueue).containsExactly(
                new IngestBatcherSubmitRequest(tableName, fileList));
    }

    private TableProperties createTableProperties(String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, tableName);
        return tableProperties;
    }

    private void addTable(String tableName) {
        TableProperties tableProperties = createTableProperties("test-table");
        sleeperClient.addTable(tableProperties, List.of());
    }

    private void ingest(String tableName, List<Record> records) {
        InMemoryIngest ingest = new InMemoryIngest(instanceProperties,
                sleeperClient.getTableProperties(tableName),
                sleeperClient.getStateStore(tableName),
                dataStore, sketchesStore);
        try (IngestCoordinator<Record> coordinator = ingest.createCoordinator()) {
            for (Record record : records) {
                coordinator.write(record);
            }
        } catch (IteratorCreationException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private RangeFactory rangeFactory() {
        return new RangeFactory(schema);
    }

    private SleeperClientBulkImport bulkImportSender() {
        return (platform, job) -> bulkImportQueues
                .computeIfAbsent(platform, p -> new LinkedList<>())
                .add(job);
    }
}
