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
package sleeper.statestore.inmemory;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.FileInfo;
import sleeper.statestore.FileInfo.FileStatus;
import sleeper.statestore.FileInfoFactory;
import sleeper.statestore.FileInfoStore;
import sleeper.statestore.StateStoreException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.statestore.FileInfo.FileStatus.GARBAGE_COLLECTION_PENDING;
import static sleeper.statestore.FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION;

public class InMemoryFileInfoStoreTest {

    @Test
    public void shouldAddAndReadActiveFiles() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        FileInfoFactory factory = FileInfoFactory.builder()
                .schema(schema)
                .partitionTree(tree)
                .lastStateStoreUpdate(Instant.now())
                .build();
        FileInfo file1 = factory.rootFile("file1", 100L, "a", "b");
        FileInfo file2 = factory.rootFile("file2", 100L, "c", "d");
        FileInfo file3 = factory.rootFile("file3", 100L, "e", "f");

        // When
        FileInfoStore store = new InMemoryFileInfoStore();
        store.addFile(file1);
        store.addFiles(Arrays.asList(file2, file3));

        // Then
        List<FileInfo> fileInfoFileInPartitionList = Arrays.asList(
                file1.cloneWithStatus(FileStatus.FILE_IN_PARTITION),
                file2.cloneWithStatus(FileStatus.FILE_IN_PARTITION),
                file3.cloneWithStatus(FileStatus.FILE_IN_PARTITION));
        assertThat(store.getFileInPartitionList()).containsExactlyInAnyOrder(fileInfoFileInPartitionList.toArray(new FileInfo[]{}));
        assertThat(store.getFileInPartitionInfosWithNoJobId()).containsExactlyInAnyOrder(fileInfoFileInPartitionList.toArray(new FileInfo[]{}));
        assertThat(store.getReadyForGCFiles()).isExhausted();
        assertThat(store.getPartitionToFileInPartitionMap())
                .containsOnlyKeys("root")
                .hasEntrySatisfying("root", files ->
                        assertThat(files).containsExactlyInAnyOrder("file1", "file2", "file3"));
    }

    @Test
    public void shouldSetStatusToReadyForGarbageCollection() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        FileInfoFactory factory = FileInfoFactory.builder()
                .schema(schema)
                .partitionTree(tree)
                .lastStateStoreUpdate(Instant.ofEpochMilli(0L))
                .build();
        FileInfo file1 = factory.rootFile("file1", 100L, "a", "b");
        FileInfo file2 = factory.rootFile("file2", 100L, "c", "d");
        FileInfo file3 = factory.rootFile("file3", 200L, "e", "f");
        FileInfoStore store = new InMemoryFileInfoStore();
        store.addFiles(Arrays.asList(file1, file2));
        store.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(Arrays.asList(file1, file2), file3);

        // When
        store.setStatusToReadyForGarbageCollection(file2.getFilename());

        // Then
        FileInfo expectedFileInfoForFile2 = file2.cloneWithStatus(READY_FOR_GARBAGE_COLLECTION);
        List<FileInfo> fileLifecyclesForFile2 = store.getFileLifecycleList().stream()
                .filter(f -> f.getFilename().equals(file2.getFilename()))
                .collect(Collectors.toList());
        assertThat(fileLifecyclesForFile2).hasSize(1);
        assertThat(fileLifecyclesForFile2.get(0).getLastStateStoreUpdateTime()).isGreaterThan(0L);
        expectedFileInfoForFile2 = expectedFileInfoForFile2.toBuilder()
                .lastStateStoreUpdateTime(fileLifecyclesForFile2.get(0).getLastStateStoreUpdateTime())
                .build();
        assertThat(expectedFileInfoForFile2).isEqualTo(fileLifecyclesForFile2.get(0));
    }

    @Test
    public void shouldThrowExceptionWhenSettingStatusToReadyForGCForFileWithNoActiveRecord() throws Exception {
        // Given
        FileInfoStore store = new InMemoryFileInfoStore();

        // When / Then
        assertThatThrownBy(() -> store.setStatusToReadyForGarbageCollection("afile"))
                .isInstanceOf(StateStoreException.class)
                .hasMessageContaining("as there is no file lifecycle record for the file");
    }

    @Test
    public void shouldThrowExceptionWhenSettingStatusToReadyForGCForFileWithFileInPartiitionRecord() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        FileInfoFactory factory = FileInfoFactory.builder().schema(schema).partitionTree(tree).build();
        FileInfo file1 = factory.rootFile("file1", 100L, "a", "b");
        FileInfoStore store = new InMemoryFileInfoStore();
        store.addFile(file1);

        // When / Then
        assertThatThrownBy(() -> store.setStatusToReadyForGarbageCollection(file1.getFilename()))
                .isInstanceOf(StateStoreException.class)
                .hasMessageContaining("there exists a FILE_IN_PARTITION record for the file");
    }

    @Test
    public void shouldReturnCorrectFileLifecycleList() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        FileInfoFactory factory = FileInfoFactory.builder().schema(schema).partitionTree(tree).build();
        FileInfo file1 = factory.rootFile("file1", 100L, "a", "b");
        FileInfo file2 = factory.rootFile("file2", 100L, "c", "d");
        FileInfo file3 = factory.rootFile("file3", 200L, "e", "f");
        FileInfoStore store = new InMemoryFileInfoStore();
        store.addFiles(Arrays.asList(file1, file2, file3));

        // When
        List<FileInfo> fileLifecycleList = store.getFileLifecycleList();

        // Then
        List<FileInfo> expectedFileInfoList = Arrays.asList(
                file1.cloneWithStatus(FileStatus.ACTIVE),
                file2.cloneWithStatus(FileStatus.ACTIVE),
                file3.cloneWithStatus(FileStatus.ACTIVE));
        assertThat(fileLifecycleList)
                .containsExactlyInAnyOrder(expectedFileInfoList.toArray(new FileInfo[0]));
    }

    @Test
    public void shouldReturnCorrectActiveFileList() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        FileInfoFactory factory = FileInfoFactory.builder().schema(schema).partitionTree(tree).build();
        FileInfo file1 = factory.rootFile("file1", 100L, "a", "b");
        FileInfo file2 = factory.rootFile("file2", 100L, "c", "d");
        FileInfo file3 = factory.rootFile("file3", 200L, "e", "f");
        FileInfo file4 = factory.rootFile("file4", 300L, "e", "h");
        FileInfoStore store = new InMemoryFileInfoStore();
        store.addFiles(Arrays.asList(file1, file2, file3));
        store.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(Collections.singletonList(file3), file4);
        store.setStatusToReadyForGarbageCollection(file3.getFilename());

        // When
        List<FileInfo> activeFileList = store.getActiveFileList();

        // Then
        List<FileInfo> expectedFileInfoList = Arrays.asList(
                file1.cloneWithStatus(FileStatus.ACTIVE),
                file2.cloneWithStatus(FileStatus.ACTIVE),
                file4.cloneWithStatus(FileStatus.ACTIVE));
        assertThat(activeFileList)
                .containsExactlyInAnyOrder(expectedFileInfoList.toArray(new FileInfo[0]));
    }

    @Test
    public void shouldReturnCorrectFileInPartitionList() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        FileInfoFactory factory = FileInfoFactory.builder()
                .schema(schema)
                .partitionTree(tree)
                .lastStateStoreUpdate(Instant.now())
                .build();
        FileInfo file1 = factory.rootFile("file1", 100L, "a", "b");
        FileInfo file2 = factory.rootFile("file2", 100L, "c", "d");
        FileInfo file3 = factory.rootFile("file3", 200L, "e", "f");
        FileInfoStore store = new InMemoryFileInfoStore();
        store.addFiles(Arrays.asList(file1, file2, file3));

        // When
        List<FileInfo> fileLifecycleList = store.getFileInPartitionList();

        // Then
        List<FileInfo> expectedFileInfoList = Arrays.asList(
                file1.cloneWithStatus(FileStatus.FILE_IN_PARTITION),
                file2.cloneWithStatus(FileStatus.FILE_IN_PARTITION),
                file3.cloneWithStatus(FileStatus.FILE_IN_PARTITION));
        assertThat(fileLifecycleList)
                .containsExactlyInAnyOrder(expectedFileInfoList.toArray(new FileInfo[0]));
    }

    @Test
    public void shouldReturnCorrectReadyForGCFilesIterator() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        FileInfoStore store = new InMemoryFileInfoStore(4);
        //  - A file which should be garbage collected immediately
        //     (NB Need to add file, which adds file-in-partition and lifecycle enrties, then simulate a compaction
        //      to remove the file in partition entries, then set the status to ready for GC)
        FileInfoFactory factory = FileInfoFactory.builder()
                .schema(schema)
                .partitionTree(tree)
                .lastStateStoreUpdate(Instant.ofEpochMilli(System.currentTimeMillis() - 8000))
                .build();
        FileInfo file1 = factory.rootFile("file1", 100L, "a", "b");
        FileInfo file2 = factory.rootFile("file2", 100L, "a", "b");
        store.addFile(file1);
        store.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(Collections.singletonList(file1.cloneWithStatus(FileStatus.FILE_IN_PARTITION)),
                file2);
        store.setStatusToReadyForGarbageCollection(file1.getFilename());
        //  - An active file which should not be garbage collected immediately
        FileInfoFactory factory2 = FileInfoFactory.builder()
                .schema(schema)
                .partitionTree(tree)
                .lastStateStoreUpdate(Instant.ofEpochMilli(System.currentTimeMillis() + 4000L))
                .build();
        FileInfo file3 = factory2.rootFile("file3", 100L, "a", "b");
        store.addFile(file3);
        FileInfo file4 = factory2.rootFile("file4", 100L, "a", "b");
        store.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(Collections.singletonList(file3.cloneWithStatus(FileStatus.FILE_IN_PARTITION)),
                file4);
        //  - A file which is ready for garbage collection but which should not be garbage collected now as it has only
        //      just been marked as ready for GC
        FileInfo file5 = factory2.rootFile("file5", 100L, "a", "b");
        store.addFile(file5);

        // When / Then 1
        Thread.sleep(5000L);
        List<String> readyForGCFiles = new ArrayList<>();
        store.getReadyForGCFiles().forEachRemaining(readyForGCFiles::add);
        assertThat(readyForGCFiles).hasSize(1);
        // file1 = file1.toBuilder()
        //         .fileStatus(FileStatus.READY_FOR_GARBAGE_COLLECTION)
        //         .lastStateStoreUpdateTime(readyForGCFiles.get(0).getLastStateStoreUpdateTime())
        //         .build();
        assertThat(readyForGCFiles.get(0)).isEqualTo("file1");

        // When / Then 2
        store.setStatusToReadyForGarbageCollection(file3.getFilename());
        Thread.sleep(5000L);
        readyForGCFiles.clear();
        store.getReadyForGCFiles().forEachRemaining(readyForGCFiles::add);
        assertThat(readyForGCFiles).hasSize(2);
        assertThat(readyForGCFiles.stream().collect(Collectors.toSet())).containsExactlyInAnyOrder("file1", "file3");
    }

    @Test
    public void shouldFindFilesThatShouldHaveStatusOfGCPending() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        FileInfoFactory factory = FileInfoFactory.builder().schema(schema).partitionTree(tree).build();
        FileInfo file1 = factory.rootFile("file1", 100L, "a", "b");
        FileInfo file2 = factory.rootFile("file2", 100L, "a", "b");
        FileInfo file3 = factory.rootFile("file3", 100L, "a", "b");
        FileInfoStore store = new InMemoryFileInfoStore();
        store.addFile(file1);
        store.addFile(file2);
        store.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(Collections.singletonList(file1), file3);

        // When
        store.findFilesThatShouldHaveStatusOfGCPending();

        // Then
        // - Check that file1 has status of GARBAGE_COLLECTION_PENDING
        FileInfo fileInfoForFile1 = store.getFileLifecycleList().stream()
                .filter(fi -> fi.getFilename().equals(file1.getFilename()))
                .findFirst()
                .get();
        assertThat(fileInfoForFile1.getFileStatus()).isEqualTo(FileInfo.FileStatus.GARBAGE_COLLECTION_PENDING);
        // - Check that file2 and file3 have statuses of ACTIVE
        List<FileInfo> fileInfoForFile2 = store.getFileLifecycleList().stream()
                .filter(fi -> fi.getFilename().equals(file2.getFilename()) || fi.getFilename().equals(file3.getFilename()))
                .collect(Collectors.toList());
        assertThat(fileInfoForFile2).extracting(FileInfo::getFileStatus).containsOnly(FileInfo.FileStatus.ACTIVE);
    }

    @Test
    public void shouldAtomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        FileInfoFactory factory = FileInfoFactory.builder().schema(schema).partitionTree(tree).build();
        FileInfo oldFile = factory.rootFile("oldFile", 100L, "a", "b");
        FileInfo newFile = factory.rootFile("newFile", 100L, "a", "b");
        FileInfoStore store = new InMemoryFileInfoStore();
        store.addFile(oldFile);

        // When
        store.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(Collections.singletonList(oldFile), newFile);

        // Then
        assertThat(store.getFileInPartitionList()).containsExactly(newFile.cloneWithStatus(FileStatus.FILE_IN_PARTITION));
        assertThat(store.getFileInPartitionInfosWithNoJobId()).containsExactly(newFile.cloneWithStatus(FileStatus.FILE_IN_PARTITION));
        assertThat(store.getPartitionToFileInPartitionMap())
                .containsOnlyKeys("root")
                .hasEntrySatisfying("root", files ->
                        assertThat(files).containsExactly("newFile"));
    }

    @Test
    public void shouldAtomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFiles() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        FileInfoFactory factory = FileInfoFactory.builder().schema(schema).partitionTree(tree).build();
        FileInfo oldFile = factory.rootFile("oldFile", 100L, "a", "c");
        FileInfo newLeftFile = factory.rootFile("newLeftFile", 100L, "a", "b");
        FileInfo newRightFile = factory.rootFile("newRightFile", 100L, "b", "c");
        FileInfoStore store = new InMemoryFileInfoStore();
        store.addFile(oldFile);

        // When
        store.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFiles(Collections.singletonList(oldFile), newLeftFile, newRightFile);

        // Then
        newLeftFile = newLeftFile.cloneWithStatus(FileStatus.FILE_IN_PARTITION);
        newRightFile = newRightFile.cloneWithStatus(FileStatus.FILE_IN_PARTITION);
        assertThat(store.getFileInPartitionList()).containsExactlyInAnyOrder(newLeftFile, newRightFile);
        assertThat(store.getFileInPartitionInfosWithNoJobId()).containsExactlyInAnyOrder(newLeftFile, newRightFile);
        assertThat(store.getPartitionToFileInPartitionMap())
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
        FileInfoFactory factory = FileInfoFactory.builder().schema(schema).partitionTree(tree).build();
        FileInfo oldFile = factory.rootFile("oldFile", 100L, "a", "b");
        FileInfo newFile = factory.rootFile("newFile", 100L, "a", "b");
        FileInfoStore store = new InMemoryFileInfoStore();
        store.addFile(oldFile);
        store.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(Collections.singletonList(oldFile), newFile);

        // When
        store.deleteFileLifecycleEntries(Collections.singletonList(oldFile.getFilename()));

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
        FileInfoFactory factory = FileInfoFactory.builder().schema(schema).partitionTree(tree).build();
        FileInfo file = factory.rootFile("file", 100L, "a", "b");
        FileInfoStore store = new InMemoryFileInfoStore();
        store.addFile(file);

        // When
        store.atomicallyUpdateJobStatusOfFiles("job", Collections.singletonList(file));

        // Then
        assertThat(store.getFileInPartitionList()).containsExactly(file.toBuilder().jobId("job").fileStatus(FileStatus.FILE_IN_PARTITION).build());
        assertThat(store.getFileInPartitionInfosWithNoJobId()).isEmpty();
    }

    @Test
    public void shouldNotMarkFileWithJobIdWhenOneIsAlreadySet() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        FileInfoFactory factory = FileInfoFactory.builder().schema(schema).partitionTree(tree).build();
        FileInfo file = factory.rootFile("file", 100L, "a", "b");
        FileInfoStore store = new InMemoryFileInfoStore();
        store.addFile(file);
        store.atomicallyUpdateJobStatusOfFiles("job1", Collections.singletonList(file));

        // When / Then
        assertThatThrownBy(() -> store.atomicallyUpdateJobStatusOfFiles("job2", Collections.singletonList(file)))
                .isInstanceOf(StateStoreException.class);
        assertThat(store.getFileInPartitionList()).containsExactly(file.toBuilder().jobId("job1").fileStatus(FileStatus.FILE_IN_PARTITION).build());
        assertThat(store.getFileInPartitionInfosWithNoJobId()).isEmpty();
    }

    @Test
    public void shouldNotUpdateOtherFilesIfOneFileAlreadyHasJobId() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        FileInfoFactory factory = FileInfoFactory.builder().schema(schema).partitionTree(tree).build();
        FileInfo file1 = factory.rootFile("file1", 100L, "a", "b")
                .toBuilder()
                .fileStatus(FileStatus.FILE_IN_PARTITION)
                .build();
        FileInfo file2 = factory.rootFile("file2", 100L, "c", "d")
                .toBuilder()
                .fileStatus(FileStatus.FILE_IN_PARTITION)
                .build();
        FileInfo file3 = factory.rootFile("file3", 100L, "e", "f")
                .toBuilder()
                .fileStatus(FileStatus.FILE_IN_PARTITION)
                .build();
        FileInfoStore store = new InMemoryFileInfoStore();
        store.addFiles(Arrays.asList(file1, file2, file3));
        store.atomicallyUpdateJobStatusOfFiles("job1", Collections.singletonList(file2));

        // When / Then
        assertThatThrownBy(() -> store.atomicallyUpdateJobStatusOfFiles("job2", Arrays.asList(file1, file2, file3)))
                .isInstanceOf(StateStoreException.class);
        assertThat(store.getFileInPartitionList()).containsExactlyInAnyOrder(
                file1, file2.toBuilder().jobId("job1").build(), file3);
        assertThat(store.getFileInPartitionInfosWithNoJobId()).containsExactlyInAnyOrder(file1, file3);
    }
}
