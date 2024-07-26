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
package sleeper.compaction.strategy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.strategy.CompactionStrategyIndex.FilesInPartition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.table.TableStatus;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.withJobId;

public class CompactionStrategyIndexTest {
    private final TableStatus tableStatus = TableStatus.uniqueIdAndName("test-table-id", "test-table", true);
    private final Schema schema = schemaWithKey("test");

    @Nested
    @DisplayName("Unassigned files")
    class UnassignedFiles {
        @Test
        void shouldIndexOneLeafPartitionWithMultipleFiles() {
            // Given
            PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema)
                    .rootFirst("root");
            FileReferenceFactory factory = FileReferenceFactory.from(partitionsBuilder.buildTree());
            FileReference file1 = factory.rootFile("file1.parquet", 456L);
            FileReference file2 = factory.rootFile("file2.parquet", 789L);
            FileReference file3 = factory.rootFile("file3.parquet", 123L);
            List<FileReference> allFileReferences = List.of(file1, file2, file3);

            // When
            CompactionStrategyIndex index = new CompactionStrategyIndex(tableStatus, allFileReferences, partitionsBuilder.buildList());

            // Then
            assertThat(index.getFilesInLeafPartitions())
                    .containsExactly(unassignedFilesInPartition("root", List.of(file3, file1, file2)));
        }

        @Test
        void shouldIndexMultipleFilesWithSameNumberOfRecords() {
            // Given
            PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema)
                    .rootFirst("root");
            FileReferenceFactory factory = FileReferenceFactory.from(partitionsBuilder.buildTree());
            FileReference file1 = factory.rootFile("file1.parquet", 100L);
            FileReference file2 = factory.rootFile("file2.parquet", 100L);
            FileReference file3 = factory.rootFile("file3.parquet", 100L);
            List<FileReference> allFileReferences = List.of(file1, file2, file3);

            // When
            CompactionStrategyIndex index = new CompactionStrategyIndex(tableStatus, allFileReferences, partitionsBuilder.buildList());

            // Then
            assertThat(index.getFilesInLeafPartitions())
                    .containsExactly(unassignedFilesInPartition("root", List.of(file1, file2, file3)));
        }

        @Test
        void shouldIndexMultipleLeafPartitionsWithMultipleFiles() {
            // Given
            PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 123L);
            FileReferenceFactory factory = FileReferenceFactory.from(partitionsBuilder.buildTree());
            FileReference file1 = factory.partitionFile("L", "file1.parquet", 120L);
            FileReference file2 = factory.partitionFile("R", "file2.parquet", 456L);
            FileReference file3 = factory.partitionFile("R", "file3.parquet", 789L);
            List<FileReference> allFileReferences = List.of(file1, file2, file3);

            // When
            CompactionStrategyIndex index = new CompactionStrategyIndex(tableStatus, allFileReferences, partitionsBuilder.buildList());

            // Then
            assertThat(index.getFilesInLeafPartitions())
                    .containsExactlyInAnyOrder(
                            unassignedFilesInPartition("L", List.of(file1)),
                            unassignedFilesInPartition("R", List.of(file2, file3)));
        }
    }

    @Test
    void shouldIgnoreLeafPartitionsWithNoFiles() {
        // Given
        PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema)
                .rootFirst("root");

        // When
        CompactionStrategyIndex index = new CompactionStrategyIndex(tableStatus, List.of(), partitionsBuilder.buildList());

        // Then
        assertThat(index.getFilesInLeafPartitions()).isEmpty();
    }

    @Test
    void shouldIgnoreLeafPartitionsWithOnlyAssignedFiles() {
        // Given
        PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema)
                .rootFirst("root");
        FileReferenceFactory factory = FileReferenceFactory.from(partitionsBuilder.buildTree());
        FileReference file = withJobId("job1", factory.rootFile("file.parquet", 120L));

        // When
        CompactionStrategyIndex index = new CompactionStrategyIndex(tableStatus, List.of(file), partitionsBuilder.buildList());

        // Then
        assertThat(index.getFilesInLeafPartitions()).isEmpty();
    }

    @Test
    void shouldIndexUnassignedFilesInLeafPartitions() {
        // Given
        PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema)
                .rootFirst("root");
        FileReferenceFactory factory = FileReferenceFactory.from(partitionsBuilder.buildTree());
        FileReference file1 = factory.rootFile("file.parquet", 120L);
        FileReference file2 = withJobId("job1", factory.rootFile("file.parquet", 120L));

        // When
        CompactionStrategyIndex index = new CompactionStrategyIndex(tableStatus, List.of(file1, file2), partitionsBuilder.buildList());

        // Then
        assertThat(index.getFilesInLeafPartitions()).containsExactly(
                new FilesInPartition(tableStatus, "root", List.of(file1), List.of(file2)));
    }

    @Test
    void shouldIgnoreFilesInNonLeafPartitions() {
        // Given
        PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 123L);
        FileReferenceFactory factory = FileReferenceFactory.from(partitionsBuilder.buildTree());
        FileReference file1 = factory.rootFile("file1.parquet", 120L);
        List<FileReference> allFileReferences = List.of(file1);

        // When
        CompactionStrategyIndex index = new CompactionStrategyIndex(tableStatus, allFileReferences, partitionsBuilder.buildList());

        // Then
        assertThat(index.getFilesInLeafPartitions()).isEmpty();
    }

    private FilesInPartition unassignedFilesInPartition(String partitionId, List<FileReference> unassignedFiles) {
        return new FilesInPartition(tableStatus, partitionId, unassignedFiles, List.of());
    }
}
