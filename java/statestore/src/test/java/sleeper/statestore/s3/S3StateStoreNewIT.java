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

package sleeper.statestore.s3;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.SplitFileReferences;
import sleeper.core.statestore.StateStoreException;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class S3StateStoreNewIT extends S3StateStoreNewTestBase {

    @Nested
    @DisplayName("Handle S3-specific cases for file references")
    class FileReferences {
        @BeforeEach
        void setUp() throws Exception {
            initialiseWithSchema(schemaWithKey("key", new LongType()));
        }

        @Test
        public void shouldAddAndRetrieve1000FileReferences() throws Exception {
            // Given
            List<FileReference> files = IntStream.range(0, 1000)
                    .mapToObj(i -> factory.rootFile("file-" + i, 1))
                    .collect(Collectors.toUnmodifiableList());
            store.fixTime(Instant.ofEpochMilli(1_000_000L));

            // When
            store.addFiles(files);

            // Then
            assertThat(new HashSet<>(store.getFileReferences())).isEqualTo(files.stream()
                    .map(reference -> withLastUpdate(Instant.ofEpochMilli(1_000_000L), reference))
                    .collect(Collectors.toSet()));
        }

        @Test
        void shouldUseOneRevisionUpdateToSplitFilesInDifferentPartitions() throws Exception {
            // Given
            splitPartition("root", "L", "R", 5);
            splitPartition("L", "LL", "LR", 2);
            splitPartition("R", "RL", "RR", 7);
            FileReference file1 = factory.partitionFile("L", "file1", 100L);
            FileReference file2 = factory.partitionFile("R", "file2", 200L);
            store.addFiles(List.of(file1, file2));

            // When
            SplitFileReferences.from(store).split();

            // Then
            assertThat(store.getFileReferences()).containsExactlyInAnyOrder(
                    splitFile(file1, "LL"),
                    splitFile(file1, "LR"),
                    splitFile(file2, "RL"),
                    splitFile(file2, "RR"));
            assertThat(getCurrentFilesRevision()).isEqualTo(versionWithPrefix("3"));
        }

        @Test
        public void shouldAddFilesUnderContention() throws Exception {
            ExecutorService executorService = Executors.newFixedThreadPool(20);
            try {
                // Given
                List<FileReference> files = IntStream.range(0, 20)
                        .mapToObj(i -> factory.rootFile("file-" + i, 1))
                        .collect(Collectors.toUnmodifiableList());

                // When
                CompletableFuture.allOf(files.stream()
                        .map(file -> (Runnable) () -> {
                            try {
                                store.addFile(file);
                            } catch (StateStoreException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .map(runnable -> CompletableFuture.runAsync(runnable, executorService))
                        .toArray(CompletableFuture[]::new)
                ).join();

                // Then
                assertThat(new HashSet<>(store.getFileReferences()))
                        .isEqualTo(new HashSet<>(files));
            } finally {
                executorService.shutdown();
            }
        }
    }

    @Nested
    @DisplayName("Initialise partitions with all key types")
    class InitialisePartitionsWithKeyTypes {
        @Test
        public void shouldStorePartitionsSplitOnLongKey() throws Exception {
            // Given
            Schema schema = schemaWithKey("key", new LongType());
            PartitionsBuilder partitions = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 100L);

            // When
            initialiseWithSchemaAndPartitions(schema, partitions);

            // Then
            assertThat(store.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions.buildList());
        }

        @Test
        public void shouldStorePartitionsSplitOnStringKey() throws Exception {
            // Given
            Schema schema = schemaWithKey("key", new StringType());
            PartitionsBuilder partitions = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "A");

            // When
            initialiseWithSchemaAndPartitions(schema, partitions);

            // Then
            assertThat(store.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions.buildList());
        }

        @Test
        public void shouldStorePartitionsSplitOnByteArrayKey() throws Exception {
            // Given
            Schema schema = schemaWithKey("key", new ByteArrayType());
            PartitionsBuilder partitions = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", new byte[]{1, 2, 3, 4});

            // When
            initialiseWithSchemaAndPartitions(schema, partitions);

            // Then
            assertThat(store.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions.buildList());
        }

        @Test
        public void shouldStorePartitionsSplitOnMultidimensionalByteArrayKey() throws Exception {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(
                            new Field("key1", new ByteArrayType()),
                            new Field("key2", new ByteArrayType()))
                    .build();
            PartitionsBuilder partitions = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildrenOnDimension("root", "L", "R", 0, new byte[]{1, 2, 3, 4})
                    .splitToNewChildrenOnDimension("L", "LL", "LR", 1, new byte[]{99, 5})
                    .splitToNewChildrenOnDimension("R", "RL", "RR", 1, new byte[]{101, 0});

            // When
            initialiseWithSchemaAndPartitions(schema, partitions);

            // Then
            assertThat(store.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions.buildList());
        }
    }

    @Nested
    @DisplayName("Store partition tree")
    class StorePartitionTree {

        @Test
        public void shouldStoreSeveralLayersOfPartitions() throws Exception {
            // Given
            Schema schema = schemaWithKey("key", new LongType());
            PartitionsBuilder partitions = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 100L)
                    .splitToNewChildren("L", "LL", "LR", 1L)
                    .splitToNewChildren("R", "RL", "RR", 200L);

            // When
            initialiseWithSchemaAndPartitions(schema, partitions);

            // Then
            assertThat(store.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions.buildList());
        }

        @Test
        public void shouldReturnLeafPartitionsAfterSplits() throws Exception {
            // Given
            Schema schema = schemaWithKey("key", new LongType());
            initialiseWithSchema(schema);

            // When
            splitPartition("root", "L", "R", 1L);
            splitPartition("R", "RL", "RR", 9L);

            // Then
            PartitionTree expectedTree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 1L)
                    .splitToNewChildren("R", "RL", "RR", 9L)
                    .buildTree();
            assertThat(store.getLeafPartitions())
                    .containsExactlyInAnyOrder(
                            expectedTree.getPartition("L"),
                            expectedTree.getPartition("RL"),
                            expectedTree.getPartition("RR"));
        }

        @Test
        public void shouldSplitAPartition() throws Exception {
            // Given
            Schema schema = schemaWithKey("key", new LongType());
            initialiseWithSchema(schema);
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "leftChild", "rightChild", 0L)
                    .buildTree();

            // When
            store.atomicallyUpdatePartitionAndCreateNewOnes(
                    tree.getPartition("root"),
                    tree.getPartition("leftChild"),
                    tree.getPartition("rightChild"));

            // Then
            assertThat(store.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(tree.getAllPartitions());
        }

        @Test
        public void shouldNotSplitAPartitionWhichHasAlreadyBeenSplit() throws Exception {
            // Given
            Schema schema = schemaWithKey("key", new LongType());
            initialiseWithSchema(schema);
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "leftChild", "rightChild", 0L)
                    .buildTree();
            store.atomicallyUpdatePartitionAndCreateNewOnes(
                    tree.getPartition("root"),
                    tree.getPartition("leftChild"),
                    tree.getPartition("rightChild"));

            // When / Then
            assertThatThrownBy(() ->
                    store.atomicallyUpdatePartitionAndCreateNewOnes(
                            tree.getPartition("root"),
                            tree.getPartition("leftChild"),
                            tree.getPartition("rightChild")))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldFailSplittingAPartitionWithWrongChildren() throws Exception {
            // Given
            Schema schema = schemaWithKey("key", new LongType());
            initialiseWithSchema(schema);
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 0L)
                    .splitToNewChildren("L", "LL", "LR", -100L)
                    .buildTree();

            // When / Then
            assertThatThrownBy(() ->
                    store.atomicallyUpdatePartitionAndCreateNewOnes(
                            tree.getPartition("root"),
                            tree.getPartition("LL"),
                            tree.getPartition("LR")))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldFailSplittingAPartitionWithChildrenOfWrongParent() throws Exception {
            // Given
            Schema schema = schemaWithKey("key", new LongType());
            initialiseWithSchema(schema);
            PartitionTree parentTree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "child1", "child2", 0L)
                    .buildTree();
            PartitionTree childrenTree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 100L)
                    .splitToNewChildren("L", "child1", "child2", 0L)
                    .buildTree();

            // When / Then
            assertThatThrownBy(() ->
                    store.atomicallyUpdatePartitionAndCreateNewOnes(
                            parentTree.getPartition("root"),
                            childrenTree.getPartition("child1"),
                            childrenTree.getPartition("child2")))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldFailSplittingAPartitionWhenNewPartitionIsNotALeaf() throws Exception {
            // Given
            Schema schema = schemaWithKey("key", new LongType());
            initialiseWithSchema(schema);
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 0L)
                    .splitToNewChildren("L", "LL", "LR", -100L)
                    .buildTree();

            // When / Then
            assertThatThrownBy(() ->
                    store.atomicallyUpdatePartitionAndCreateNewOnes(
                            tree.getPartition("root"),
                            tree.getPartition("L"), // Not a leaf
                            tree.getPartition("R")))
                    .isInstanceOf(StateStoreException.class);
        }
    }

    private String getCurrentFilesRevision() {
        S3RevisionIdStore revisionStore = new S3RevisionIdStore(dynamoDBClient, instanceProperties, tableProperties);
        return revisionStore.getCurrentFilesRevisionId().getRevision();
    }

    private static String versionWithPrefix(String version) {
        return "00000000000" + version;
    }
}
