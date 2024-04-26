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

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.transactionlog.StateStoreFiles;
import sleeper.core.statestore.transactionlog.StateStorePartitions;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TRANSACTION_LOG_LOAD_LATEST_SNAPSHOTS;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AllReferencesToAFile.fileWithOneReference;

public class TransactionLogStateStoreDynamoDBSpecificIT extends TransactionLogStateStoreTestBase {
    @TempDir
    private Path tempDir;
    private final Schema schema = schemaWithKey("key", new LongType());
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private TransactionLogSnapshotSerDe snapshotSerDe = new TransactionLogSnapshotSerDe(schema, new Configuration());
    private DynamoDBTransactionLogSnapshotStore snapshotStore = new DynamoDBTransactionLogSnapshotStore(instanceProperties, tableProperties, dynamoDBClient);

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
            PartitionTree tree = new PartitionsBuilder(schema)
                    .leavesWithSplits(leafIds, splitPoints)
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
            PartitionTree tree = new PartitionsBuilder(schema)
                    .leavesWithSplits(leafIds, splitPoints)
                    .anyTreeJoiningAllLeaves().buildTree();

            // When
            stateStore.initialise(tree.getAllPartitions());

            // Then
            assertThat(createStateStore().getAllPartitions()).containsExactlyElementsOf(tree.getAllPartitions());
        }
    }

    @Nested
    @DisplayName("Load latest snapshots")
    class LoadLatestSnapshots {
        @Test
        void shouldLoadLatestSnapshotsWhenCreatingStateStore() throws Exception {
            // Given
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 123L)
                    .buildTree();
            FileReferenceFactory factory = FileReferenceFactory.from(tree);
            List<FileReference> files = List.of(
                    factory.rootFile("file1.parquet", 100L),
                    factory.partitionFile("L", "file2.parquet", 25L),
                    factory.partitionFile("R", "file3.parquet", 50L));
            saveFilesSnapshot(files, 2);
            savePartitionsSnapshot(tree, 3);

            // When
            StateStore stateStore = createStateStore();

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyElementsOf(tree.getAllPartitions());
            assertThat(stateStore.getFileReferences())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                    .containsExactlyElementsOf(files);
        }

        @Test
        void shouldNotLoadLatestSnapshotsIfPropertyIsFalse() throws Exception {
            // Given
            tableProperties.set(TRANSACTION_LOG_LOAD_LATEST_SNAPSHOTS, "false");
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 123L)
                    .buildTree();
            FileReferenceFactory factory = FileReferenceFactory.from(tree);
            List<FileReference> files = List.of(
                    factory.rootFile("file1.parquet", 100L),
                    factory.partitionFile("L", "file2.parquet", 25L),
                    factory.partitionFile("R", "file3.parquet", 50L));
            saveFilesSnapshot(files, 2);
            savePartitionsSnapshot(tree, 3);

            // When
            StateStore stateStore = createStateStore();

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
            FileReferenceFactory factory1 = FileReferenceFactory.from(tree1);
            FileReference file1 = factory1.rootFile("file1.parquet", 123L);
            stateStore.initialise(tree1.getAllPartitions());
            stateStore.addFile(file1);

            PartitionTree tree2 = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "C", "D", 456L)
                    .buildTree();
            FileReferenceFactory factory2 = FileReferenceFactory.from(tree2);
            FileReference file2 = factory2.rootFile("file2.parquet", 456L);
            saveFilesSnapshot(List.of(file2), 2);
            savePartitionsSnapshot(tree2, 2);

            // When
            stateStore = createStateStore();

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyElementsOf(tree2.getAllPartitions());
            assertThat(stateStore.getFileReferences())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                    .containsExactly(file2);
        }

        @Test
        void shouldLoadLatestPartitionsSnapshotIfNoFilesSnapshotIsPresent() throws Exception {
            // Given
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 123L)
                    .buildTree();
            savePartitionsSnapshot(tree, 3);

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
            FileReferenceFactory factory = FileReferenceFactory.from(tree);
            List<FileReference> files = List.of(
                    factory.rootFile("file1.parquet", 100L),
                    factory.partitionFile("L", "file2.parquet", 25L),
                    factory.partitionFile("R", "file3.parquet", 50L));
            saveFilesSnapshot(files, 2);

            // When
            StateStore stateStore = createStateStore();

            // Then
            assertThat(stateStore.getAllPartitions()).isEmpty();
            assertThat(stateStore.getFileReferences())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                    .containsExactlyElementsOf(files);
        }
    }

    private void saveFilesSnapshot(List<FileReference> files, long transactionNumber) throws Exception {
        StateStoreFiles state = new StateStoreFiles();
        files.forEach(file -> state.add(fileWithOneReference(file, Instant.now())));
        TransactionLogSnapshot snapshot = TransactionLogSnapshot.forFiles(tempDir.toString(), transactionNumber);
        snapshotSerDe.saveFiles(snapshot, state);
        snapshotStore.saveSnapshot(snapshot);
    }

    private void savePartitionsSnapshot(PartitionTree partitionTree, long transactionNumber) throws Exception {
        StateStorePartitions state = new StateStorePartitions();
        partitionTree.getAllPartitions().forEach(state::put);
        TransactionLogSnapshot snapshot = TransactionLogSnapshot.forPartitions(tempDir.toString(), transactionNumber);
        snapshotSerDe.savePartitions(snapshot, state);
        snapshotStore.saveSnapshot(snapshot);
    }

    private StateStore createStateStore() {
        return DynamoDBTransactionLogStateStore.builderFrom(instanceProperties, tableProperties, dynamoDBClient, s3Client, configuration)
                .maxAddTransactionAttempts(1)
                .build();
    }
}
