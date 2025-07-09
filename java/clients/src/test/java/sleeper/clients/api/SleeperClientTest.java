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
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequest;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.query.core.model.Query;
import sleeper.query.core.recordretrieval.QueryExecutor;

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
        assertThatThrownBy(() -> instance.sleeperClientBuilder().recordRetrieverProvider(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("recordRetrieverProvider");
        assertThatThrownBy(() -> instance.sleeperClientBuilder().ingestJobSender(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("ingestJobSender");
        assertThatThrownBy(() -> instance.sleeperClientBuilder().bulkImportJobSender(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("bulkImportJobSender");
        assertThatThrownBy(() -> instance.sleeperClientBuilder().ingestBatcherSender(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("ingestBatcherSender");
        assertThatThrownBy(() -> instance.sleeperClientBuilder().bulkExportQuerySender(null).build())
                .isInstanceOf(NullPointerException.class).hasMessageContaining("bulkExportQuerySender");
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
        QueryExecutor queryExecutor = sleeperClient.getQueryExecutor("test-table");

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
        TableProperties tableProperties = createTableProperties("test-table");
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

    private RangeFactory rangeFactory() {
        return new RangeFactory(schema);
    }
}
