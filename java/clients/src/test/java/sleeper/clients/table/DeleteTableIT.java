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

package sleeper.clients.table;

import org.apache.commons.io.FilenameUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.record.Row;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.log.TransactionLogRange;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.table.TableStatus;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.core.IngestResult;
import sleeper.ingest.runner.IngestFactory;
import sleeper.ingest.runner.IngestRecords;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStore;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotCreator;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotMetadataStore;
import sleeper.statestore.transactionlog.snapshots.LatestSnapshots;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.core.table.TableStatusTestHelper.uniqueIdAndName;

public class DeleteTableIT extends LocalStackTestBase {
    @TempDir
    private Path tempDir;

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = createSchemaWithKey("key1");
    private final TablePropertiesStore propertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);
    private String inputFolderName;

    @BeforeEach
    void setUp() throws IOException {
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        createBucket(instanceProperties.get(DATA_BUCKET));
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        inputFolderName = createTempDirectory(tempDir, null).toString();
    }

    @Test
    void shouldDeleteOnlyTable() throws Exception {
        // Given
        TableProperties table = createTable(uniqueIdAndName("test-table-1", "table-1"));
        StateStore stateStoreBefore = createStateStore(table);
        update(stateStoreBefore).initialise(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 50L)
                .buildList());
        AllReferencesToAFile file = ingestRecords(table, List.of(
                new Row(Map.of("key1", 25L)),
                new Row(Map.of("key1", 100L))));
        assertThat(listDataBucketObjectKeys())
                .extracting(FilenameUtils::getName)
                .containsExactly(
                        FilenameUtils.getName(file.getFilename()),
                        FilenameUtils.getName(file.getFilename()).replace("parquet", "sketches"));

        // When
        deleteTable("table-1");

        // Then
        assertThatThrownBy(() -> propertiesStore.loadByName("table-1"))
                .isInstanceOf(TableNotFoundException.class);
        assertThat(listDataBucketObjectKeys()).isEmpty();
        assertThat(streamTableFileTransactions(table)).isEmpty();
        assertThat(streamTablePartitionTransactions(table)).isEmpty();
    }

    @Test
    void shouldDeleteOneTableWhenAnotherTableIsPresent() throws Exception {
        // Given
        TableProperties table1 = createTable(uniqueIdAndName("test-table-1", "table-1"));
        StateStore stateStore1 = createStateStore(table1);
        update(stateStore1).initialise(schema);
        AllReferencesToAFile file1 = ingestRecords(table1, List.of(
                new Row(Map.of("key1", 25L))));
        assertThat(listDataBucketObjectKeys())
                .extracting(FilenameUtils::getName)
                .containsExactly(
                        FilenameUtils.getName(file1.getFilename()),
                        FilenameUtils.getName(file1.getFilename()).replace("parquet", "sketches"));
        TableProperties table2 = createTable(uniqueIdAndName("test-table-2", "table-2"));
        StateStore stateStore2 = createStateStore(table2);
        update(stateStore2).initialise(schema);
        AllReferencesToAFile file2 = ingestRecords(table2, List.of(new Row(Map.of("key1", 25L))));

        // When
        deleteTable("table-1");

        // Then
        assertThatThrownBy(() -> propertiesStore.loadByName("table-1"))
                .isInstanceOf(TableNotFoundException.class);
        assertThat(streamTableFileTransactions(table1)).isEmpty();
        assertThat(streamTablePartitionTransactions(table1)).isEmpty();
        assertThat(propertiesStore.loadByName("table-2"))
                .isEqualTo(table2);
        assertThat(streamTableFileTransactions(table2)).isNotEmpty();
        assertThat(streamTablePartitionTransactions(table2)).isNotEmpty();
        assertThat(listDataBucketObjectKeys())
                .extracting(FilenameUtils::getName)
                .containsExactly(
                        FilenameUtils.getName(file2.getFilename()),
                        FilenameUtils.getName(file2.getFilename()).replace("parquet", "sketches"));
    }

    @Test
    void shouldDeleteTableWhenSnapshotIsPresent() throws Exception {
        // Given
        TableProperties table = createTable(uniqueIdAndName("test-table-1", "table-1"));
        StateStore stateStore = createStateStore(table);
        update(stateStore).initialise(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 50L)
                .buildList());
        AllReferencesToAFile file = ingestRecords(table, List.of(
                new Row(Map.of("key1", 25L)),
                new Row(Map.of("key1", 100L))));

        DynamoDBTransactionLogSnapshotCreator.from(instanceProperties, table, s3Client, s3TransferManager, dynamoClient)
                .createSnapshot();

        assertThat(listDataBucketObjectKeys())
                .extracting(FilenameUtils::getName)
                .containsExactly(
                        // Data files
                        FilenameUtils.getName(file.getFilename()),
                        FilenameUtils.getName(file.getFilename()).replace("parquet", "sketches"),
                        // Snapshot files
                        "1-files.arrow",
                        "1-partitions.arrow");

        // When
        deleteTable("table-1");

        // Then
        assertThatThrownBy(() -> propertiesStore.loadByName("table-1"))
                .isInstanceOf(TableNotFoundException.class);
        assertThat(listDataBucketObjectKeys()).isEmpty();
        // And
        var snapshotMetadataStore = snapshotMetadataStore(table);
        assertThat(snapshotMetadataStore.getLatestSnapshots()).isEqualTo(LatestSnapshots.empty());
        assertThat(snapshotMetadataStore.getFilesSnapshots()).isEmpty();
        assertThat(snapshotMetadataStore.getPartitionsSnapshots()).isEmpty();
    }

    @Test
    void shouldFailToDeleteTableThatDoesNotExist() {
        // When / Then
        assertThatThrownBy(() -> deleteTable("table-1"))
                .isInstanceOf(TableNotFoundException.class);
    }

    private void deleteTable(String tableName) throws Exception {
        new DeleteTable(instanceProperties, s3Client, dynamoClient)
                .delete(tableName);
    }

    private TableProperties createTable(TableStatus tableStatus) {
        TableProperties table = createTestTableProperties(instanceProperties, schema);
        table.set(TABLE_ID, tableStatus.getTableUniqueId());
        table.set(TABLE_NAME, tableStatus.getTableName());
        propertiesStore.save(table);
        return table;
    }

    private StateStore createStateStore(TableProperties tableProperties) {
        return new StateStoreFactory(instanceProperties, s3Client, dynamoClient).getStateStore(tableProperties);
    }

    private AllReferencesToAFile ingestRecords(TableProperties tableProperties, List<Row> records) throws Exception {
        IngestFactory factory = IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(inputFolderName)
                .stateStoreProvider(StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient))
                .s3AsyncClient(s3AsyncClient)
                .instanceProperties(instanceProperties)
                .hadoopConfiguration(hadoopConf)
                .build();

        IngestRecords ingestRecords = factory.createIngestRecords(tableProperties);
        ingestRecords.init();
        for (Row record : records) {
            ingestRecords.write(record);
        }
        IngestResult result = ingestRecords.close();
        List<AllReferencesToAFile> files = AllReferencesToAFile.newFilesWithReferences(result.getFileReferenceList());
        if (files.size() != 1) {
            throw new IllegalStateException("Expected one file ingested, found " + files.size());
        }
        return files.get(0);
    }

    private List<String> listDataBucketObjectKeys() {
        return s3Client.listObjects(ListObjectsRequest.builder()
                .bucket(instanceProperties.get(DATA_BUCKET))
                .build())
                .contents().stream()
                .map(S3Object::key)
                .toList();
    }

    private Stream<TransactionLogEntry> streamTableFileTransactions(TableProperties tableProperties) {
        return DynamoDBTransactionLogStore.forFiles(instanceProperties, tableProperties, dynamoClient, s3Client)
                .readTransactions(TransactionLogRange.fromMinimum(1));
    }

    private Stream<TransactionLogEntry> streamTablePartitionTransactions(TableProperties tableProperties) {
        return DynamoDBTransactionLogStore.forPartitions(instanceProperties, tableProperties, dynamoClient, s3Client)
                .readTransactions(TransactionLogRange.fromMinimum(1));
    }

    private DynamoDBTransactionLogSnapshotMetadataStore snapshotMetadataStore(TableProperties tableProperties) {
        return new DynamoDBTransactionLogSnapshotMetadataStore(instanceProperties, tableProperties, dynamoClient);
    }
}
