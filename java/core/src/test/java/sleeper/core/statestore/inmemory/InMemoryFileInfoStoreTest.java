/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.core.statestore.inmemory;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.FileInfoStore;
import sleeper.core.statestore.StateStoreException;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class InMemoryFileInfoStoreTest {
    private static final String TABLE_ID = "test-table-id";

    @Test
    public void shouldAddAndReadActiveFiles() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        Instant fixedUpdateTime = Instant.parse("2023-10-04T14:08:00Z");
        FileInfoFactory factory = FileInfoFactory.fromUpdatedAt(tree, fixedUpdateTime);
        FileInfo file1 = factory.rootFile("file1", 100L);
        FileInfo file2 = factory.rootFile("file2", 100L);
        FileInfo file3 = factory.rootFile("file3", 100L);

        // When
        FileInfoStore store = new InMemoryFileInfoStore(TABLE_ID);
        store.fixTime(fixedUpdateTime);
        store.addFile(file1);
        store.addFiles(Arrays.asList(file2, file3));

        // Then
        assertThat(store.getActiveFiles()).containsExactlyInAnyOrder(file1, file2, file3);
        assertThat(store.getActiveFilesWithNoJobId()).containsExactlyInAnyOrder(file1, file2, file3);
        assertThat(store.getReadyForGCFiles()).isExhausted();
        assertThat(store.getPartitionToActiveFilesMap())
                .containsOnlyKeys("root")
                .hasEntrySatisfying("root", files ->
                        assertThat(files).containsExactlyInAnyOrder("file1", "file2", "file3"));
    }

    @Test
    public void shouldSetFileReadyForGC() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        Instant fixedUpdateTime = Instant.parse("2023-10-04T14:08:00Z");
        FileInfoFactory factory = FileInfoFactory.fromUpdatedAt(tree, fixedUpdateTime);
        FileInfo oldFile = factory.rootFile("oldFile", 100L);
        FileInfo newFile = factory.rootFile("newFile", 100L);
        FileInfoStore store = new InMemoryFileInfoStore();
        store.fixTime(fixedUpdateTime);
        store.addFile(oldFile);

        // When
        store.atomicallyRemoveFileReferencesAndCreateNewFileReferences(List.of(oldFile), newFile);

        // Then
        assertThat(store.getActiveFiles()).containsExactly(newFile);
        assertThat(store.getActiveFilesWithNoJobId()).containsExactly(newFile);
        assertThat(store.getReadyForGCFiles()).toIterable().containsExactly(oldFile);
        assertThat(store.getPartitionToActiveFilesMap())
                .containsOnlyKeys("root")
                .hasEntrySatisfying("root", files ->
                        assertThat(files).containsExactly("newFile"));
    }

    @Test
    public void shouldSetFileReadyForGCWhenSplitting() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        Instant fixedUpdateTime = Instant.parse("2023-10-04T14:08:00Z");
        FileInfoFactory factory = FileInfoFactory.fromUpdatedAt(tree, fixedUpdateTime);
        FileInfo oldFile = factory.rootFile("oldFile", 100L);
        FileInfo newLeftFile = factory.rootFile("newLeftFile", 100L);
        FileInfo newRightFile = factory.rootFile("newRightFile", 100L);
        FileInfoStore store = new InMemoryFileInfoStore();
        store.fixTime(fixedUpdateTime);
        store.addFile(oldFile);

        // When
        store.atomicallyRemoveFileReferencesAndCreateNewFileReferences(List.of(oldFile), newLeftFile, newRightFile);

        // Then
        assertThat(store.getActiveFiles())
                .containsExactlyInAnyOrder(newLeftFile, newRightFile);
        assertThat(store.getActiveFilesWithNoJobId())
                .containsExactlyInAnyOrder(newLeftFile, newRightFile);
        assertThat(store.getReadyForGCFiles()).toIterable().containsExactly(oldFile);
        assertThat(store.getPartitionToActiveFilesMap())
                .containsOnlyKeys("root")
                .hasEntrySatisfying("root", files ->
                        assertThat(files).containsExactlyInAnyOrder("newLeftFile", "newRightFile"));
    }

    @Test
    public void shouldDeleteGarbageCollectedFile() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        FileInfoFactory factory = FileInfoFactory.from(tree);
        FileInfo oldFile = factory.rootFile("oldFile", 100L);
        FileInfo newFile = factory.rootFile("newFile", 100L);
        FileInfoStore store = new InMemoryFileInfoStore();
        store.addFile(oldFile);
        store.atomicallyRemoveFileReferencesAndCreateNewFileReferences(List.of(oldFile), newFile);

        // When
        store.deleteReadyForGCFile(oldFile);

        // Then
        assertThat(store.getReadyForGCFiles()).isExhausted();
    }

    @Test
    public void shouldMarkFileWithJobId() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        Instant fixedUpdateTime = Instant.parse("2023-10-04T14:08:00Z");
        FileInfoFactory factory = FileInfoFactory.fromUpdatedAt(tree, fixedUpdateTime);
        FileInfo file = factory.rootFile("file", 100L);
        FileInfoStore store = new InMemoryFileInfoStore();
        store.fixTime(fixedUpdateTime);
        store.addFile(file);

        // When
        store.atomicallyUpdateJobStatusOfFiles("job", Collections.singletonList(file));

        // Then
        assertThat(store.getActiveFiles()).containsExactly(file.toBuilder().jobId("job").build());
        assertThat(store.getActiveFilesWithNoJobId()).isEmpty();
    }

    @Test
    public void shouldNotMarkFileWithJobIdWhenOneIsAlreadySet() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        Instant fixedUpdateTime = Instant.parse("2023-10-04T14:08:00Z");
        FileInfoFactory factory = FileInfoFactory.fromUpdatedAt(tree, fixedUpdateTime);
        FileInfo file = factory.rootFile("file", 100L);
        FileInfoStore store = new InMemoryFileInfoStore();
        store.fixTime(fixedUpdateTime);
        store.addFile(file);
        store.atomicallyUpdateJobStatusOfFiles("job1", Collections.singletonList(file));

        // When / Then
        assertThatThrownBy(() -> store.atomicallyUpdateJobStatusOfFiles("job2", Collections.singletonList(file)))
                .isInstanceOf(StateStoreException.class);
        assertThat(store.getActiveFiles()).containsExactly(file.toBuilder().jobId("job1").build());
        assertThat(store.getActiveFilesWithNoJobId()).isEmpty();
    }

    @Test
    public void shouldNotUpdateOtherFilesIfOneFileAlreadyHasJobId() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        Instant fixedUpdateTime = Instant.parse("2023-10-04T14:08:00Z");
        FileInfoFactory factory = FileInfoFactory.fromUpdatedAt(tree, fixedUpdateTime);
        FileInfo file1 = factory.rootFile("file1", 100L);
        FileInfo file2 = factory.rootFile("file2", 100L);
        FileInfo file3 = factory.rootFile("file3", 100L);
        FileInfoStore store = new InMemoryFileInfoStore();
        store.fixTime(fixedUpdateTime);
        store.addFiles(Arrays.asList(file1, file2, file3));
        store.atomicallyUpdateJobStatusOfFiles("job1", Collections.singletonList(file2));

        // When / Then
        assertThatThrownBy(() -> store.atomicallyUpdateJobStatusOfFiles("job2", Arrays.asList(file1, file2, file3)))
                .isInstanceOf(StateStoreException.class);
        assertThat(store.getActiveFiles()).containsExactlyInAnyOrder(
                file1, file2.toBuilder().jobId("job1").build(), file3);
        assertThat(store.getActiveFilesWithNoJobId()).containsExactlyInAnyOrder(file1, file3);
    }

    @Test
    void shouldSetLastUpdateTimeForFileWhenFixingTimeCorrectly() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        Instant file1Time = Instant.parse("2023-10-04T14:08:00Z");
        FileInfoFactory factory = FileInfoFactory.fromUpdatedAt(tree, file1Time);
        FileInfo file1 = factory.rootFile("file1", 100L);

        // When
        FileInfoStore store = new InMemoryFileInfoStore();
        store.fixTime(file1Time);
        store.addFile(file1);

        // Then
        assertThat(store.getActiveFiles()).containsExactlyInAnyOrder(file1);
    }

    @Test
    void shouldClearStore() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema).singlePartition("root").buildTree();
        FileInfoFactory factory = FileInfoFactory.from(tree);
        FileInfo oldFile = factory.rootFile("oldFile", 100L);
        FileInfo newFile = factory.rootFile("newFile", 100L);
        FileInfoStore store = new InMemoryFileInfoStore();
        store.addFile(oldFile);
        store.atomicallyRemoveFileReferencesAndCreateNewFileReferences(List.of(oldFile), newFile);

        // When
        store.clearTable();

        // Then
        assertThat(store.getActiveFiles()).isEmpty();
        assertThat(store.getReadyForGCFiles()).isExhausted();
        assertThat(store.hasNoFiles()).isTrue();
    }
}
