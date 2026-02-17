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
package sleeper.cdk.custom;

import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import org.apache.commons.io.FilenameUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.log.TransactionLogRange;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.core.IngestResult;
import sleeper.ingest.runner.IngestFactory;
import sleeper.ingest.runner.IngestRows;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStore;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotCreator;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotMetadataStore;
import sleeper.statestore.transactionlog.snapshots.LatestSnapshots;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.DEFAULT_RETAIN_TABLE_AFTER_REMOVAL;
import static sleeper.core.properties.table.TableProperty.RETAIN_TABLE_AFTER_REMOVAL;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.core.properties.table.TableProperty.TABLE_REUSE_EXISTING;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class TableDefinerLambdaIT extends LocalStackTestBase {

    private static final String CREATE = "Create", UPDATE = "Update", DELETE = "Delete";
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = createSchemaWithKey("key1");
    private final TablePropertiesStore propertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);
    private final TableDefinerLambda tableDefinerLambda = new TableDefinerLambda(s3Client, dynamoClient, instanceProperties.get(CONFIG_BUCKET));
    private final TableProperties tableProperties = createTableProperties();

    @BeforeEach
    void setUp() throws IOException {
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
    }

    private TableProperties createTableProperties() {
        TableProperties properties = createTestTableProperties(instanceProperties, schema);
        // Properties from the CDK will not have a table ID.
        // The user will not set this as it's created internally by Sleeper.
        properties.unset(TABLE_ID);
        return properties;
    }

    private void callLambda(String type, TableProperties tableProperties) throws IOException {
        callLambda(type, tableProperties, "");
    }

    private void callLambda(String type, TableProperties tableProperties, String splitPoints) throws IOException {
        HashMap<String, Object> resourceProperties = new HashMap<>();
        resourceProperties.put("tableProperties", tableProperties.saveAsString());
        resourceProperties.put("splitPoints", splitPoints);

        handleEvent(type, resourceProperties);
    }

    private void callLambda(String type, TableProperties tableProperties, String splitPoints, TableProperties existingTableProperties) throws IOException {
        HashMap<String, Object> resourceProperties = new HashMap<>();
        resourceProperties.put("tableProperties", tableProperties.saveAsString());
        resourceProperties.put("splitPoints", splitPoints);
        resourceProperties.put("existingTableProperties", existingTableProperties.saveAsString());

        handleEvent(type, resourceProperties);
    }

    private void handleEvent(String type, HashMap<String, Object> resourceProperties) throws IOException {
        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType(type)
                .withResourceProperties(resourceProperties)
                .build();

        tableDefinerLambda.handleEvent(event, null);
    }

    @Nested
    class TableDefinerLambdaCreateIT {

        private StateStore stateStore(TableProperties tableProperties) {
            return new StateStoreFactory(instanceProperties, s3Client, dynamoClient).getStateStore(tableProperties);
        }

        @Test
        public void shouldCreateTableWithNoSplitPoints() throws IOException {
            // Given/ When
            callLambda(CREATE, tableProperties, "");

            // Then
            assertThat(propertiesStore.streamAllTables()).singleElement().satisfies(foundProperties -> {
                assertThat(withoutTableId(foundProperties)).isEqualTo(tableProperties);
                assertThat(stateStore(foundProperties).getAllPartitions())
                        .containsExactlyElementsOf(new PartitionsBuilder(schema)
                                .rootFirst("root")
                                .buildList());
            });
        }

        @Test
        public void shouldCreateTableWithMultipleSplitPoints() throws IOException {
            // Given/When
            callLambda(CREATE, tableProperties, "0\n5\n10\n");

            // Then
            assertThat(propertiesStore.streamAllTables()).singleElement().satisfies(foundProperties -> {
                assertThat(withoutTableId(foundProperties)).isEqualTo(tableProperties);
                assertThat(stateStore(foundProperties).getAllPartitions())
                        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id", "parentPartitionId", "childPartitionIds")
                        .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                                .rootFirst("root")
                                .splitToNewChildren("root", "l", "r", Long.valueOf(5))
                                .splitToNewChildren("l", "ll", "lr", Long.valueOf(0))
                                .splitToNewChildren("r", "rl", "rr", Long.valueOf(10))
                                .buildList());
            });
        }

        @Test
        public void shouldFailToReuseTableIfTableDoesNotExist() throws IOException {
            //Given
            tableProperties.set(TABLE_REUSE_EXISTING, "true");

            //Then
            assertThatThrownBy(() -> callLambda(CREATE, tableProperties))
                    .isInstanceOf(NoTableToReuseException.class);
        }

        @Test
        public void shouldFailToReuseTableIfImportFlagNotSet() throws IOException {
            //Given
            tableProperties.set(TABLE_NAME, "tableName");
            tableProperties.set(TABLE_ID, "tableID");
            callLambda(CREATE, tableProperties);
            //Should just take the table offline
            tableProperties.set(RETAIN_TABLE_AFTER_REMOVAL, "true");
            callLambda(DELETE, tableProperties);

            //Then
            assertThatThrownBy(() -> callLambda(CREATE, tableProperties))
                    .isInstanceOf(TableAlreadyExistsException.class)
                    .hasMessage("Table already exists: tableName (tableID) [offline]. If attempting to reuse an " +
                            "existing table ensure the sleeper.table.reuse.existing property is set to true.");
        }

        @Test
        public void shouldUpdateTableIfImportDataPropertySet() throws IOException {
            //Given
            callLambda(CREATE, tableProperties);
            //Should just take the table offline
            tableProperties.set(RETAIN_TABLE_AFTER_REMOVAL, "true");
            callLambda(DELETE, tableProperties);
            assertThat(propertiesStore.loadByName(tableProperties.get(TABLE_NAME)).getBoolean(TABLE_ONLINE)).isFalse();

            //When
            tableProperties.set(TABLE_ONLINE, "true");
            tableProperties.set(TABLE_REUSE_EXISTING, "true");
            callLambda(CREATE, tableProperties);

            //Then
            assertThat(propertiesStore.streamAllTables()).singleElement()
                    .extracting(table -> withoutTableId(table))
                    .isEqualTo(tableProperties);
        }
    }

    @Nested
    class TableDefinerLambdaUpdateIT {

        @Test
        public void shouldUpdateTablePropertiesSuccessfully() throws IOException {
            //Given
            callLambda(CREATE, tableProperties);
            tableProperties.set(TABLE_ONLINE, "false");

            //When
            callLambda(UPDATE, tableProperties);

            //Then
            assertThat(propertiesStore.streamAllTables()).singleElement()
                    .extracting(table -> withoutTableId(table))
                    .isEqualTo(tableProperties);
        }

        @Test
        public void shouldRenameTableSuccessfully() throws IOException {
            //Given
            tableProperties.set(TABLE_NAME, "old-table-name");
            callLambda(CREATE, tableProperties);

            TableProperties existingTableProperties = TableProperties.copyOf(tableProperties);
            tableProperties.set(TABLE_NAME, "new-table-name");

            //When
            callLambda(UPDATE, tableProperties, "", existingTableProperties);

            //Then
            assertThat(propertiesStore.streamAllTables()).singleElement()
                    .extracting(table -> withoutTableId(table))
                    .isEqualTo(tableProperties);
        }
    }

    @Nested
    class TableDefinerLambdaDeleteIT {
        private String inputFolderName;

        @TempDir
        private Path tempDir;

        @BeforeEach
        void setUp() throws IOException {
            createBucket(instanceProperties.get(DATA_BUCKET));
            inputFolderName = createTempDirectory(tempDir, null).toString();
        }

        @Test
        public void shouldFailToDeleteTableThatDoesNotExist() {
            // Given
            instanceProperties.set(DEFAULT_RETAIN_TABLE_AFTER_REMOVAL, "false");
            S3InstanceProperties.saveToS3(s3Client, instanceProperties);

            // When / Then
            assertThatThrownBy(() -> callLambda(DELETE, tableProperties))
                    .isInstanceOf(TableNotFoundException.class);
        }

        @Test
        void shouldTakeTableOfflineWhenDeleteCalledAndInstancePropertySetTrue() throws Exception {
            // Given
            tableProperties.set(TABLE_ONLINE, "true");
            instanceProperties.set(DEFAULT_RETAIN_TABLE_AFTER_REMOVAL, "true");
            S3InstanceProperties.saveToS3(s3Client, instanceProperties);

            callLambda(CREATE, tableProperties);
            TableProperties createdProperties = propertiesStore.loadByName(tableProperties.get(TABLE_NAME));
            AllReferencesToAFile file = ingestRows(createdProperties, List.of(new Row(Map.of("key1", 25L))));

            // When
            callLambda(DELETE, tableProperties);

            // Then
            tableProperties.set(TABLE_ONLINE, "false");
            assertThat(propertiesStore.streamAllTables()).singleElement().satisfies(foundProperties -> {
                assertThat(withoutTableId(foundProperties)).isEqualTo(tableProperties);
                assertThat(streamTableFileTransactions(foundProperties)).isNotEmpty();
                assertThat(streamTablePartitionTransactions(foundProperties)).isNotEmpty();
                assertThat(listDataBucketObjectKeys())
                        .extracting(FilenameUtils::getName)
                        .containsExactly(
                                FilenameUtils.getName(file.getFilename()),
                                FilenameUtils.getName(file.getFilename()).replace("parquet", "sketches"));
            });
        }

        @Test
        void shouldFullyDeleteSpecifiedTableButNoOthers() throws Exception {
            // Given
            instanceProperties.set(DEFAULT_RETAIN_TABLE_AFTER_REMOVAL, "false");
            S3InstanceProperties.saveToS3(s3Client, instanceProperties);

            TableProperties table1 = createTableProperties("table-1");
            callLambda(CREATE, table1);
            TableProperties createdTable1 = propertiesStore.loadByName(table1.get(TABLE_NAME));

            AllReferencesToAFile file1 = ingestRows(createdTable1, List.of(
                    new Row(Map.of("key1", 25L))));
            assertThat(listDataBucketObjectKeys())
                    .extracting(FilenameUtils::getName)
                    .containsExactly(
                            FilenameUtils.getName(file1.getFilename()),
                            FilenameUtils.getName(file1.getFilename()).replace("parquet", "sketches"));

            TableProperties table2 = createTableProperties("table-2");
            callLambda(CREATE, table2);
            TableProperties createdTable2 = propertiesStore.loadByName(table2.get(TABLE_NAME));
            AllReferencesToAFile file2 = ingestRows(createdTable2, List.of(new Row(Map.of("key1", 25L))));

            // When
            callLambda(DELETE, table1);

            // Then
            assertThat(propertiesStore.streamAllTables()).singleElement()
                    .extracting(table -> withoutTableId(table))
                    .isEqualTo(table2);
            assertThat(streamTableFileTransactions(createdTable1)).isEmpty();
            assertThat(streamTablePartitionTransactions(createdTable1)).isEmpty();
            assertThat(streamTableFileTransactions(createdTable2)).isNotEmpty();
            assertThat(streamTablePartitionTransactions(createdTable2)).isNotEmpty();
            assertThat(listDataBucketObjectKeys())
                    .extracting(FilenameUtils::getName)
                    .containsExactly(
                            FilenameUtils.getName(file2.getFilename()),
                            FilenameUtils.getName(file2.getFilename()).replace("parquet", "sketches"));
        }

        @Test
        void shouldFullyDeleteTableWhenSnapshotIsPresent() throws Exception {
            // Given
            instanceProperties.set(DEFAULT_RETAIN_TABLE_AFTER_REMOVAL, "false");
            S3InstanceProperties.saveToS3(s3Client, instanceProperties);

            callLambda(CREATE, tableProperties, "50");
            TableProperties createdTableProperties = propertiesStore.loadByName(tableProperties.get(TABLE_NAME));

            AllReferencesToAFile file = ingestRows(createdTableProperties, List.of(
                    new Row(Map.of("key1", 25L)),
                    new Row(Map.of("key1", 100L))));

            createStateStoreSnapshot(createdTableProperties);

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
            callLambda(DELETE, tableProperties);

            // Then
            assertThat(propertiesStore.streamAllTables()).isEmpty();
            assertThat(listDataBucketObjectKeys()).isEmpty();
            // And
            var snapshotMetadataStore = snapshotMetadataStore(createdTableProperties);
            assertThat(snapshotMetadataStore.getLatestSnapshots()).isEqualTo(LatestSnapshots.empty());
            assertThat(snapshotMetadataStore.getFilesSnapshots()).isEmpty();
            assertThat(snapshotMetadataStore.getPartitionsSnapshots()).isEmpty();
        }

        private TableProperties createTableProperties(String tableName) {
            TableProperties table = createTestTableProperties(instanceProperties, schema);
            table.unset(TABLE_ID);
            table.set(TABLE_NAME, tableName);
            return table;
        }

        private AllReferencesToAFile ingestRows(TableProperties tableProperties, List<Row> rows) throws Exception {
            IngestFactory factory = IngestFactory.builder()
                    .objectFactory(ObjectFactory.noUserJars())
                    .localDir(inputFolderName)
                    .stateStoreProvider(StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient))
                    .s3AsyncClient(s3AsyncClient)
                    .instanceProperties(instanceProperties)
                    .hadoopConfiguration(hadoopConf)
                    .build();

            IngestRows ingestRows = factory.createIngestRows(tableProperties);
            ingestRows.init();
            for (Row row : rows) {
                ingestRows.write(row);
            }
            IngestResult result = ingestRows.close();
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

    private static TableProperties withoutTableId(TableProperties tableProperties) {
        TableProperties withoutTableId = TableProperties.copyOf(tableProperties);
        withoutTableId.unset(TABLE_ID);
        return withoutTableId;
    }

    private void createStateStoreSnapshot(TableProperties tableProperties) {
        DynamoDBTransactionLogSnapshotCreator.from(instanceProperties, tableProperties, s3Client, s3TransferManager, dynamoClient)
                .createSnapshot();
    }
}
