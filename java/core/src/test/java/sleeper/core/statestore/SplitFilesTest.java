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

package sleeper.core.statestore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithNoPartitions;

public class SplitFilesTest {
    private static final Instant DEFAULT_UPDATE_TIME = Instant.parse("2023-10-04T14:08:00Z");
    private final Schema schema = schemaWithKey("key", new LongType());
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");
    private FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    private final StateStore store = inMemoryStateStoreWithNoPartitions();

    @BeforeEach
    void setUp() {
        store.fixTime(DEFAULT_UPDATE_TIME);
    }

    @Test
    void shouldSplitOneFileInNonLeafPartition() throws Exception {
        // Given
        splitPartition("root", "L", "R", 5L);
        FileReference file = factory.rootFile("to-split.parquet", 100L);
        store.addFile(file);

        // When
        SplitFiles splitFiles = SplitFiles.from(store);
        splitFiles.split();

        // Then
        assertThat(store.getActiveFiles())
                .containsExactlyInAnyOrder(
                        splitFile(file, "L"),
                        splitFile(file, "R"));
    }

    @Test
    void shouldNotSplitOneFileInLeafPartition() throws Exception {
        // Given
        splitPartition("root", "L", "R", 5L);
        FileReference file = factory.partitionFile("L", "already-split.parquet", 100L);
        store.addFile(file);

        // When
        SplitFiles splitFiles = SplitFiles.from(store);
        splitFiles.split();

        // Then
        assertThat(store.getActiveFiles())
                .containsExactly(file);
    }

    @Test
    void shouldSplitTwoFilesInDifferentPartitions() throws Exception {
        // Given
        splitPartition("root", "L", "R", 5L);
        splitPartition("L", "LL", "LR", 2L);
        splitPartition("R", "RL", "RR", 7L);
        FileReference file1 = factory.partitionFile("L", "file1.parquet", 100L);
        FileReference file2 = factory.partitionFile("R", "file2.parquet", 100L);
        store.addFiles(List.of(file1, file2));

        // When
        SplitFiles splitFiles = SplitFiles.from(store);
        splitFiles.split();

        // Then
        assertThat(store.getActiveFiles())
                .containsExactlyInAnyOrder(
                        splitFile(file1, "LL"),
                        splitFile(file1, "LR"),
                        splitFile(file2, "RL"),
                        splitFile(file2, "RR"));
    }

    @Test
    void shouldOnlyPerformOneLevelOfSplits() throws Exception {
        // Given
        splitPartition("root", "L", "R", 5L);
        splitPartition("L", "LL", "LR", 2L);
        splitPartition("R", "RL", "RR", 7L);
        FileReference file = factory.rootFile("file.parquet", 100L);
        store.addFile(file);

        // When
        SplitFiles splitFiles = SplitFiles.from(store);
        splitFiles.split();

        // Then
        assertThat(store.getActiveFiles())
                .containsExactlyInAnyOrder(
                        splitFile(file, "L"),
                        splitFile(file, "R"));
    }

    private void splitPartition(String parentId, String leftId, String rightId, long splitPoint) throws StateStoreException {
        partitions.splitToNewChildren(parentId, leftId, rightId, splitPoint)
                .applySplit(store, parentId);
        factory = FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    }

    private FileReference splitFile(FileReference parentFile, String childPartitionId) {
        return SplitFileReference.referenceForChildPartition(parentFile, childPartitionId)
                .toBuilder().lastStateStoreUpdateTime(DEFAULT_UPDATE_TIME).build();
    }
}
