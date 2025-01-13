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
package sleeper.statestore.transactionlog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsBuilderSplitsFirst;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogSnapshotSetup;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogSnapshotSetup.SetupStateStore;
import sleeper.core.statestore.transactionlog.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.statestore.transactionlog.transactions.AddFilesTransaction;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotCreator;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotMetadataStore;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.ADD_TRANSACTION_MAX_ATTEMPTS;
import static sleeper.core.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.transactionlog.InMemoryTransactionLogSnapshotSetup.setupSnapshotWithFreshState;

public class TransactionLogStateStoreDynamoDBSpecificIT extends TransactionLogStateStoreTestBase {
    @TempDir
    private Path tempDir;
    private final Schema schema = schemaWithKey("key", new LongType());
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);

    @BeforeEach
    void setUp() {
        tableProperties.setNumber(ADD_TRANSACTION_MAX_ATTEMPTS, 1);
    }

    @Nested
    @DisplayName("Handle large transactions")
    class HandleLargeTransactions {
        private StateStore stateStore;

        @BeforeEach
        public void setup() {
            stateStore = createStateStore();
        }

        @Test
        void shouldInitialiseTableWithManyPartitionsCreatingTransactionTooLargeToFitInADynamoDBItem() throws Exception {
            // Given
            List<String> leafIds = IntStream.range(0, 1000)
                    .mapToObj(i -> "" + i)
                    .collect(toUnmodifiableList());
            List<Object> splitPoints = LongStream.range(1, 1000)
                    .mapToObj(i -> i)
                    .collect(toUnmodifiableList());
            PartitionTree tree = PartitionsBuilderSplitsFirst
                    .leavesWithSplits(schema, leafIds, splitPoints)
                    .anyTreeJoiningAllLeaves().buildTree();

            // When
            stateStore.initialise(tree.getAllPartitions());

            // Then
            assertThat(stateStore.getAllPartitions()).containsExactlyElementsOf(tree.getAllPartitions());
        }

        @Test
        void shouldReadTransactionTooLargeToFitInADynamoDBItemWithFreshStateStoreInstance() throws Exception {
            // Given
            List<String> leafIds = IntStream.range(0, 1000)
                    .mapToObj(i -> "" + i)
                    .collect(toUnmodifiableList());
            List<Object> splitPoints = LongStream.range(1, 1000)
                    .mapToObj(i -> i)
                    .collect(toUnmodifiableList());
            PartitionTree tree = PartitionsBuilderSplitsFirst
                    .leavesWithSplits(schema, leafIds, splitPoints)
                    .anyTreeJoiningAllLeaves().buildTree();

            // When
            stateStore.initialise(tree.getAllPartitions());

            // Then
            assertThat(createStateStore().getAllPartitions()).containsExactlyElementsOf(tree.getAllPartitions());
        }

        @Test
        void shouldAddATransactionAlreadyHeldInS3() {
            // Given
            TransactionLogStateStore stateStore = (TransactionLogStateStore) this.stateStore;
            PartitionTree tree = new PartitionsBuilder(schema).singlePartition("root").buildTree();
            stateStore.initialise(tree.getAllPartitions());
            FileReference file = fileFactory(tree).rootFile("test.parquet", 100);
            FileReferenceTransaction transaction = new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file)));
            String key = S3TransactionBodyStore.createObjectKey(instanceProperties, tableProperties);
            TransactionBodyStore transactionBodyStore = new S3TransactionBodyStore(instanceProperties, tableProperties, s3Client);

            // When
            transactionBodyStore.store(key, transaction);
            stateStore.addTransaction(AddTransactionRequest.transactionInBucket(key, transaction));

            // Then
            assertThat(createStateStore().getFileReferences()).containsExactly(file);
        }
    }

    @Nested
    @DisplayName("Load latest snapshots")
    class LoadLatestSnapshots {

        @Test
        void shouldLoadLatestSnapshotsWhenCreatingStateStore() throws Exception {
            // Given
            tableProperties.set(STATESTORE_CLASSNAME, DynamoDBTransactionLogStateStore.class.getName());
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 123L)
                    .buildTree();
            FileReferenceFactory factory = fileFactory(tree);
            List<FileReference> files = List.of(
                    factory.rootFile("file1.parquet", 100L),
                    factory.partitionFile("L", "file2.parquet", 25L),
                    factory.partitionFile("R", "file3.parquet", 50L));
            createSnapshotWithFreshState(stateStore -> {
                stateStore.initialise(tree.getAllPartitions());
                stateStore.addFiles(files);
            });

            // When
            StateStore stateStore = stateStoreFactory().getStateStore(tableProperties);

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyElementsOf(tree.getAllPartitions());
            assertThat(stateStore.getFileReferences())
                    .containsExactlyElementsOf(files);
        }

        @Test
        void shouldNotLoadLatestSnapshotsByClassname() throws Exception {
            // Given
            tableProperties.set(STATESTORE_CLASSNAME, DynamoDBTransactionLogStateStoreNoSnapshots.class.getName());
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 123L)
                    .buildTree();
            FileReferenceFactory factory = fileFactory(tree);
            List<FileReference> files = List.of(
                    factory.rootFile("file1.parquet", 100L),
                    factory.partitionFile("L", "file2.parquet", 25L),
                    factory.partitionFile("R", "file3.parquet", 50L));
            createSnapshotWithFreshState(stateStore -> {
                stateStore.initialise(tree.getAllPartitions());
                stateStore.addFiles(files);
            });

            // When
            StateStore stateStore = stateStoreFactory().getStateStore(tableProperties);

            // Then
            assertThat(stateStore.getAllPartitions()).isEmpty();
            assertThat(stateStore.getFileReferences()).isEmpty();
        }

        @Test
        void shouldExcludePreviousTransactionsWhenLoadingLatestSnapshots() throws Exception {
            // Given
            StateStore stateStore = createStateStore();
            PartitionTree tree1 = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "A", "B", 123L)
                    .buildTree();
            FileReferenceFactory factory1 = fileFactory(tree1);
            FileReference file1 = factory1.rootFile("file1.parquet", 123L);
            stateStore.initialise(tree1.getAllPartitions());
            stateStore.addFile(file1);

            PartitionTree tree2 = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "C", "D", 456L)
                    .buildTree();
            FileReferenceFactory factory2 = fileFactory(tree2);
            FileReference file2 = factory2.rootFile("file2.parquet", 456L);
            createSnapshotWithFreshState(stateStore2 -> {
                stateStore2.initialise(tree2.getAllPartitions());
                stateStore2.addFile(file2);
            });

            // When
            stateStore = createStateStore();

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyElementsOf(tree2.getAllPartitions());
            assertThat(stateStore.getFileReferences())
                    .containsExactly(file2);
        }

        @Test
        void shouldLoadLatestPartitionsSnapshotIfNoFilesSnapshotIsPresent() throws Exception {
            // Given
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 123L)
                    .buildTree();
            createSnapshotWithFreshState(stateStore -> {
                stateStore.initialise(tree.getAllPartitions());
            });

            // When
            StateStore stateStore = createStateStore();

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyElementsOf(tree.getAllPartitions());
            assertThat(stateStore.getFileReferences()).isEmpty();
        }

        @Test
        void shouldLoadLatestFilesSnapshotIfNoPartitionsSnapshotIsPresent() throws Exception {
            // Given
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 123L)
                    .buildTree();
            FileReferenceFactory factory = fileFactory(tree);
            List<FileReference> files = List.of(
                    factory.rootFile("file1.parquet", 100L),
                    factory.partitionFile("L", "file2.parquet", 25L),
                    factory.partitionFile("R", "file3.parquet", 50L));
            createSnapshotWithFreshState(stateStore -> {
                stateStore.addFiles(files);
            });

            // When
            StateStore stateStore = createStateStore();

            // Then
            assertThat(stateStore.getAllPartitions()).isEmpty();
            assertThat(stateStore.getFileReferences())
                    .containsExactlyElementsOf(files);
        }

        private void createSnapshotWithFreshState(SetupStateStore setupState) throws Exception {
            InMemoryTransactionLogSnapshotSetup snapshotSetup = setupSnapshotWithFreshState(
                    tableProperties.getStatus(), tableProperties.getSchema(), setupState);

            DynamoDBTransactionLogSnapshotMetadataStore snapshotStore = new DynamoDBTransactionLogSnapshotMetadataStore(
                    instanceProperties, tableProperties, dynamoDBClient);
            new DynamoDBTransactionLogSnapshotCreator(
                    instanceProperties, tableProperties,
                    snapshotSetup.getFilesLog(), snapshotSetup.getPartitionsLog(),
                    configuration, snapshotStore::getLatestSnapshots, snapshotStore::saveSnapshot)
                    .createSnapshot();
        }
    }

    private StateStore createStateStore() {
        return createStateStore(tableProperties);
    }

    private StateStoreFactory stateStoreFactory() {
        return new StateStoreFactory(instanceProperties, s3Client, dynamoDBClient, configuration);
    }

    private FileReferenceFactory fileFactory(PartitionTree tree) {
        return FileReferenceFactory.fromUpdatedAt(tree, DEFAULT_UPDATE_TIME);
    }
}
