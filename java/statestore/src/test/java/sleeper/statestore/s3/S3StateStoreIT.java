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
package sleeper.statestore.s3;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class S3StateStoreIT extends S3StateStoreTestBase {

    @Test
    public void shouldReturnCorrectFileInfoForLongRowKey() throws Exception {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo = FileInfo.wholeFile()
                .filename("abc")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .numberOfRecords(1L)
                .build();
        stateStore.fixTime(Instant.ofEpochMilli(1_000_000L));

        // When
        stateStore.addFile(fileInfo);

        // Then
        assertThat(stateStore.getActiveFiles()).singleElement().satisfies(found -> {
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileInfo.FileStatus.ACTIVE);
            assertThat(found.getPartitionId()).isEqualTo("1");
            assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        });
    }

    @Test
    public void shouldReturnCorrectFileInfoForByteArrayKey() throws Exception {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new ByteArrayType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo = FileInfo.wholeFile()
                .filename("abc")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .numberOfRecords(1L)
                .build();
        stateStore.fixTime(Instant.ofEpochMilli(1_000_000L));

        // When
        stateStore.addFile(fileInfo);

        // Then
        assertThat(stateStore.getActiveFiles()).singleElement().satisfies(found -> {
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileInfo.FileStatus.ACTIVE);
            assertThat(found.getPartitionId()).isEqualTo("1");
            assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        });
    }

    @Test
    public void shouldReturnCorrectFileInfoFor2DimensionalByteArrayKey() throws Exception {
        // Given
        Schema schema = schemaWithTwoRowKeyTypes(new ByteArrayType(), new ByteArrayType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo = FileInfo.wholeFile()
                .filename("abc")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .numberOfRecords(1L)
                .build();
        stateStore.fixTime(Instant.ofEpochMilli(1_000_000L));

        // When
        stateStore.addFile(fileInfo);

        // Then
        assertThat(stateStore.getActiveFiles()).singleElement().satisfies(found -> {
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileInfo.FileStatus.ACTIVE);
            assertThat(found.getPartitionId()).isEqualTo("1");
            assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        });
    }

    @Test
    public void shouldReturnCorrectFileInfoForMultidimensionalRowKey() throws Exception {
        // Given
        Schema schema = schemaWithTwoRowKeyTypes(new LongType(), new StringType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo = FileInfo.wholeFile()
                .filename("abc")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .numberOfRecords(1L)
                .build();
        stateStore.fixTime(Instant.ofEpochMilli(1_000_000L));

        // When
        stateStore.addFile(fileInfo);

        // Then
        assertThat(stateStore.getActiveFiles()).singleElement().satisfies(found -> {
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileInfo.FileStatus.ACTIVE);
            assertThat(found.getPartitionId()).isEqualTo("1");
            assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        });
    }

    @Test
    public void shouldReturnAllFileInfos() throws Exception {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        List<FileInfo> files = new ArrayList<>();
        Set<FileInfo> expected = new HashSet<>();
        stateStore.fixTime(Instant.ofEpochMilli(1_000_000L));
        for (int i = 0; i < 10000; i++) {
            FileInfo fileInfo = FileInfo.wholeFile()
                    .filename("file-" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId("" + i)
                    .numberOfRecords(1L)
                    .build();
            files.add(fileInfo);
            expected.add(fileInfo.toBuilder().lastStateStoreUpdateTime(1_000_000L).build());
        }
        stateStore.addFiles(files);

        // When
        List<FileInfo> fileInfos = stateStore.getActiveFiles();

        // Then
        assertThat(new HashSet<>(fileInfos)).isEqualTo(expected);
    }

    @Test
    void shouldStoreAndReturnPartialFile() throws Exception {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        stateStore.fixTime(Instant.ofEpochMilli(1_000_000L));
        FileInfo fileInfo = FileInfo.partialFile()
                .filename("partial-file")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("A")
                .numberOfRecords(123L)
                .build();
        stateStore.addFile(fileInfo);

        // When
        List<FileInfo> fileInfos = stateStore.getActiveFiles();

        // Then
        assertThat(fileInfos)
                .containsExactly(fileInfo.toBuilder().lastStateStoreUpdateTime(1_000_000L).build());
    }

    @Test
    public void shouldAddFilesUnderContention() throws Exception {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            FileInfo fileInfo = FileInfo.wholeFile()
                    .filename("file-" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId("root")
                    .numberOfRecords(1L)
                    .build();
            files.add(fileInfo);
        }

        // When
        CompletableFuture.allOf(files.stream()
                .map(file -> (Runnable) () -> {
                    try {
                        stateStore.addFile(file);
                    } catch (StateStoreException e) {
                        throw new RuntimeException(e);
                    }
                })
                .map(runnable -> CompletableFuture.runAsync(runnable, executorService))
                .toArray(CompletableFuture[]::new)
        ).join();

        // Then
        assertThat(stateStore.getActiveFiles())
                .hasSize(20)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrderElementsOf(files);
        executorService.shutdown();
    }

    @Test
    public void shouldGetFilesThatAreReadyForGC() throws Exception {
        // Given
        Instant file1Time = Instant.parse("2023-06-06T15:00:00Z");
        Instant file2Time = Instant.parse("2023-06-06T15:01:00Z");
        Instant file3Time = Instant.parse("2023-06-06T15:02:00Z");
        Instant file1GCTime = Instant.parse("2023-06-06T15:05:30Z");
        Instant file3GCTime = Instant.parse("2023-06-06T15:07:30Z");
        Schema schema = schemaWithKeyAndValueWithTypes(new IntType(), new StringType());
        S3StateStore stateStore = getStateStore(schema, 5);
        Partition partition = stateStore.getAllPartitions().get(0);
        //  - A file which should be garbage collected immediately
        FileInfo fileInfo1 = FileInfo.wholeFile()
                .filename("file1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(partition.getId())
                .numberOfRecords(100L)
                .build();
        stateStore.addFile(fileInfo1);
        //  - An active file which should not be garbage collected
        FileInfo fileInfo2 = FileInfo.wholeFile()
                .filename("file2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(partition.getId())
                .numberOfRecords(100L)
                .build();
        stateStore.fixTime(file2Time);
        stateStore.addFile(fileInfo2);
        //  - A file which is ready for garbage collection but which should not be garbage collected now as it has only
        //      just been marked as ready for GC
        FileInfo fileInfo3 = FileInfo.wholeFile()
                .filename("file3")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(partition.getId())
                .numberOfRecords(100L)
                .build();
        stateStore.addFile(fileInfo3);
        stateStore.fixTime(file1Time);
        stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List.of(fileInfo1), List.of());
        stateStore.fixTime(file3Time);
        stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List.of(fileInfo3), List.of());

        // When 1
        stateStore.fixTime(file1GCTime);
        // Then 1
        assertThat(stateStore.getReadyForGCFilenamesBefore(file2Time))
                .containsExactly("file1");
        // When 2
        stateStore.fixTime(file3GCTime);
        // Then 2
        assertThat(stateStore.getReadyForGCFilenamesBefore(file3Time.plus(Duration.ofMinutes(1))))
                .containsExactlyInAnyOrder("file1", "file3");
    }

    @Test
    public void shouldReturnOnlyActiveFilesWithNoJobId() throws Exception {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo1 = FileInfo.wholeFile()
                .filename("file1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .numberOfRecords(1L)
                .build();
        stateStore.addFile(fileInfo1);
        FileInfo fileInfo2 = FileInfo.wholeFile()
                .filename("file2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("2")
                .numberOfRecords(2L)
                .build();
        stateStore.addFile(fileInfo2);
        FileInfo fileInfo3 = FileInfo.wholeFile()
                .filename("file3")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("3")
                .jobId("job1")
                .numberOfRecords(3L)
                .build();
        stateStore.addFile(fileInfo3);

        // When
        List<FileInfo> fileInfos = stateStore.getActiveFilesWithNoJobId();

        // Then
        assertThat(fileInfos)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(fileInfo1, fileInfo2);
    }

    @Test
    public void shouldDeleteReadyForGCFile() throws Exception {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        FileInfo oldFile = FileInfo.wholeFile()
                .filename("oldFile")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("4")
                .numberOfRecords(1L)
                .build();
        FileInfo newFile = FileInfo.wholeFile()
                .filename("newFile")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("5")
                .numberOfRecords(2L)
                .build();
        stateStore.addFiles(List.of(oldFile));
        stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(List.of(oldFile), newFile);

        // When
        stateStore.deleteReadyForGCFile("oldFile");

        // Then
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(newFile);
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE))).isEmpty();
    }

    @Test
    public void shouldNotDeleteReadyForGCFileIfNotMarkedAsReadyForGC() throws Exception {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        FileInfo oldFile = FileInfo.wholeFile()
                .filename("oldFile")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("4")
                .numberOfRecords(1L)
                .build();
        FileInfo newFile = FileInfo.wholeFile()
                .filename("newFile")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("5")
                .numberOfRecords(2L)
                .build();
        stateStore.addFiles(List.of(oldFile));
        stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(List.of(oldFile), newFile);

        // When
        assertThatThrownBy(() -> stateStore.deleteReadyForGCFile("newFile"))
                .isInstanceOf(StateStoreException.class);
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)))
                .containsExactly("oldFile");
    }

    @Test
    public void shouldAtomicallyUpdateStatusToReadyForGCAndCreateNewActiveFile() throws Exception {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        List<FileInfo> filesToMoveToReadyForGC = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.wholeFile()
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId("7")
                    .numberOfRecords(1L)
                    .build();
            filesToMoveToReadyForGC.add(fileInfo);
            stateStore.addFile(fileInfo);
        }
        FileInfo newFileInfo = FileInfo.wholeFile()
                .filename("file-new")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("7")
                .numberOfRecords(4L)
                .build();

        // When
        stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(filesToMoveToReadyForGC, newFileInfo);

        // Then
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(newFileInfo);
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)))
                .containsExactlyInAnyOrder("file1", "file2", "file3", "file4");
    }

    @Test
    public void shouldAtomicallyUpdateStatusToReadyForGCAndCreateNewActiveFilesForSplittingJob() throws Exception {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        List<FileInfo> filesToMoveToReadyForGC = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.wholeFile()
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId("7")
                    .numberOfRecords((long) i)
                    .build();
            filesToMoveToReadyForGC.add(fileInfo);
        }
        stateStore.addFiles(filesToMoveToReadyForGC);
        FileInfo newLeftFileInfo = FileInfo.wholeFile()
                .filename("file-left-new")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("7")
                .numberOfRecords(5L)
                .build();
        FileInfo newRightFileInfo = FileInfo.wholeFile()
                .filename("file-right-new")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("7")
                .numberOfRecords(5L)
                .build();

        // When
        stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(filesToMoveToReadyForGC, newLeftFileInfo, newRightFileInfo);

        // Then
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(newLeftFileInfo, newRightFileInfo);
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)))
                .containsExactlyInAnyOrder("file1", "file2", "file3", "file4");
    }

    @Test
    public void atomicallyUpdateStatusToReadyForGCAndCreateNewActiveFileShouldFailIfFilesNotActive() throws Exception {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        List<FileInfo> filesToMoveToReadyForGC = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.wholeFile()
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId("7")
                    .numberOfRecords(1L)
                    .build();
            filesToMoveToReadyForGC.add(fileInfo);
        }
        //  - One of the files is not active
        FileInfo readyForGCFile = filesToMoveToReadyForGC.get(3);
        stateStore.addFiles(filesToMoveToReadyForGC);
        FileInfo newFileInfo1 = FileInfo.wholeFile()
                .filename("file-new-1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("7")
                .numberOfRecords(1L)
                .build();
        stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(List.of(readyForGCFile), newFileInfo1);
        FileInfo newFileInfo2 = FileInfo.wholeFile()
                .filename("file-new-2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("7")
                .numberOfRecords(1L)
                .build();

        // When / Then
        assertThatThrownBy(() ->
                stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(filesToMoveToReadyForGC, newFileInfo2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void atomicallyUpdateStatusToReadyForGCAndCreateNewActiveFilesShouldFailIfFilesNotActive() throws Exception {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        List<FileInfo> filesToMoveToReadyForGC = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.wholeFile()
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId("7")
                    .numberOfRecords((long) i)
                    .build();
            filesToMoveToReadyForGC.add(fileInfo);
        }
        stateStore.addFiles(filesToMoveToReadyForGC);
        FileInfo newLeftFileInfo = FileInfo.wholeFile()
                .filename("file-left-new")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("7")
                .numberOfRecords(5L)
                .build();
        FileInfo newRightFileInfo = FileInfo.wholeFile()
                .filename("file-right-new")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("7")
                .numberOfRecords(5L)
                .build();
        //  - One of the files is not active
        stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(filesToMoveToReadyForGC.subList(3, 4), List.of());

        // When / Then
        assertThatThrownBy(() ->
                stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(filesToMoveToReadyForGC, newLeftFileInfo, newRightFileInfo))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldAtomicallyUpdateJobStatusOfFiles() throws Exception {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.wholeFile()
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId("8")
                    .numberOfRecords(1L)
                    .build();
            files.add(fileInfo);
        }
        stateStore.addFiles(files);
        String jobId = UUID.randomUUID().toString();

        // When
        stateStore.atomicallyUpdateJobStatusOfFiles(jobId, files);

        // Then
        assertThat(stateStore.getActiveFiles()).hasSize(4)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("jobId", "lastStateStoreUpdateTime")
                .containsExactlyInAnyOrderElementsOf(files)
                .extracting(FileInfo::getJobId).containsOnly(jobId);
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE))).isEmpty();
    }

    @Test
    public void shouldNotAtomicallyCreateJobAndUpdateJobStatusOfFilesWhenJobIdAlreadySet() throws Exception {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.wholeFile()
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId("9")
                    .jobId("compactionJob")
                    .numberOfRecords(1L)
                    .build();
            files.add(fileInfo);
        }
        stateStore.addFiles(files);
        String jobId = UUID.randomUUID().toString();

        // When / Then
        assertThatThrownBy(() ->
                stateStore.atomicallyUpdateJobStatusOfFiles(jobId, files))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldCorrectlyInitialisePartitionsWithLongKeyType() throws Exception {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Collections.singletonList(100L))
                .construct();
        StateStore stateStore = getStateStore(schema, partitions);

        // When / Then
        assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
    }

    @Test
    public void shouldCorrectlyInitialisePartitionsWithStringKeyType() throws Exception {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new StringType());
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Collections.singletonList("A"))
                .construct();
        StateStore stateStore = getStateStore(schema, partitions);

        // When / Then
        assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
    }

    @Test
    public void shouldCorrectlyInitialisePartitionsWithByteArrayKeyType() throws Exception {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new ByteArrayType());
        byte[] min = new byte[]{1, 2, 3, 4};
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, List.of(min))
                .construct();
        StateStore stateStore = getStateStore(schema, partitions);

        // When / Then
        assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
    }

    @Test
    public void shouldCorrectlyStorePartitionWithMultidimensionalKeyType() throws Exception {
        // Given
        Field field1 = new Field("key1", new ByteArrayType());
        Field field2 = new Field("key2", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field1, field2).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        byte[] min1 = new byte[]{1, 2, 3, 4};
        byte[] min2 = new byte[]{99, 5};
        byte[] max1 = new byte[]{5, 6, 7, 8, 9};
        byte[] max2 = new byte[]{101, 0};
        Range range1 = rangeFactory.createRange(field1, min1, max1);
        Range range2 = rangeFactory.createRange(field2, min2, max2);
        Region region = new Region(Arrays.asList(range1, range2));
        Partition partition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region)
                .id("id")
                .leafPartition(true)
                .parentPartitionId("P")
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        StateStore stateStore = getStateStore(schema, Collections.singletonList(partition));

        // When
        Partition retrievedPartition = stateStore.getAllPartitions().get(0);

        // Then
        assertThat((byte[]) retrievedPartition.getRegion().getRange("key1").getMin()).containsExactly((byte[]) partition.getRegion().getRange("key1").getMin());
        assertThat((byte[]) retrievedPartition.getRegion().getRange("key1").getMax()).containsExactly((byte[]) partition.getRegion().getRange("key1").getMax());
        assertThat((byte[]) retrievedPartition.getRegion().getRange("key2").getMin()).containsExactly((byte[]) partition.getRegion().getRange("key2").getMin());
        assertThat((byte[]) retrievedPartition.getRegion().getRange("key2").getMax()).containsExactly((byte[]) partition.getRegion().getRange("key2").getMax());
        assertThat(retrievedPartition.getId()).isEqualTo(partition.getId());
        assertThat(retrievedPartition.getParentPartitionId()).isEqualTo(partition.getParentPartitionId());
        assertThat(retrievedPartition.getChildPartitionIds()).isEqualTo(partition.getChildPartitionIds());
    }

    @Test
    public void shouldCorrectlyStoreNonLeafPartitionWithByteArrayKeyType() throws Exception {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        byte[] min = new byte[]{1, 2, 3, 4};
        byte[] max = new byte[]{5, 6, 7, 8, 9};
        Range range = new RangeFactory(schema).createRange(field.getName(), min, max);
        Region region = new Region(range);
        Partition partition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region)
                .id("id")
                .leafPartition(false)
                .parentPartitionId("P")
                .childPartitionIds(new ArrayList<>())
                .dimension(0)
                .build();
        StateStore stateStore = getStateStore(schema, Collections.singletonList(partition));

        // When
        Partition retrievedPartition = stateStore.getAllPartitions().get(0);

        // Then
        assertThat((byte[]) retrievedPartition.getRegion().getRange("key").getMin()).containsExactly((byte[]) partition.getRegion().getRange("key").getMin());
        assertThat((byte[]) retrievedPartition.getRegion().getRange("key").getMax()).containsExactly((byte[]) partition.getRegion().getRange("key").getMax());
        assertThat(retrievedPartition.getId()).isEqualTo(partition.getId());
        assertThat(retrievedPartition.getParentPartitionId()).isEqualTo(partition.getParentPartitionId());
        assertThat(retrievedPartition.getChildPartitionIds()).isEqualTo(partition.getChildPartitionIds());
        assertThat(retrievedPartition.getDimension()).isEqualTo(partition.getDimension());
    }

    @Test
    public void shouldReturnCorrectPartitionToFileMapping() throws Exception {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore stateStore = getStateStore(schema);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            FileInfo fileInfo = FileInfo.wholeFile()
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId("" + (i % 5))
                    .numberOfRecords((long) i)
                    .build();
            files.add(fileInfo);
        }
        stateStore.addFiles(files);

        // When
        Map<String, List<String>> partitionToFileMapping = stateStore.getPartitionToActiveFilesMap();

        // Then
        assertThat(partitionToFileMapping.entrySet()).hasSize(5);
        for (int i = 0; i < 5; i++) {
            assertThat(partitionToFileMapping.get("" + i)).hasSize(2);
            Set<String> expected = new HashSet<>();
            expected.add(files.get(i).getFilename());
            expected.add(files.get(i + 5).getFilename());
            assertThat(new HashSet<>(partitionToFileMapping.get("" + i))).isEqualTo(expected);
        }
    }

    @Test
    public void shouldReturnAllPartitions() throws Exception {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();

        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 100L)
                .splitToNewChildren("left", "id0", "id2", 1L)
                .splitToNewChildren("right", "id1", "id3", 200L)
                .buildTree();
        S3StateStore stateStore = getStateStore(schema, tree.getAllPartitions());
        // When / Then
        assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(tree.getAllPartitions());
    }

    @Test
    public void shouldReturnLeafPartitionsAfterPartitionUpdate() throws Exception {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();

        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .buildTree();
        S3StateStore stateStore = getStateStore(schema, List.of(tree.getRootPartition()));
        PartitionTree stepOneTree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "id1", "id2", 1L)
                .buildTree();

        PartitionTree expectedTree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "id1", "id2", 1L)
                .splitToNewChildren("id2", "id3", "id4", 9L)
                .buildTree();

        // When
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(stepOneTree.getRootPartition(), stepOneTree.getPartition("id1"), stepOneTree.getPartition("id2"));
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(expectedTree.getPartition("id2"), expectedTree.getPartition("id3"), expectedTree.getPartition("id4"));

        // Then
        assertThat(stateStore.getLeafPartitions())
                .containsExactlyInAnyOrderElementsOf(expectedTree.getAllPartitions().stream().filter(Partition::isLeafPartition).collect(Collectors.toList()));
    }

    @Test
    public void shouldUpdatePartitions() throws Exception {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .buildTree();
        S3StateStore stateStore = getStateStore(schema, tree.getAllPartitions());

        // When
        PartitionTree expectedTree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "child1", "child2", 0L)
                .buildTree();
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(expectedTree.getRootPartition(), expectedTree.getPartition("child1"), expectedTree.getPartition("child2"));

        // Then
        assertThat(stateStore.getAllPartitions())
                .containsExactlyInAnyOrderElementsOf(expectedTree.getAllPartitions());
    }

    @Test
    public void shouldNotUpdatePartitionsIfLeafStatusChanges() throws Exception {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "child1", "child2", 0L)
                .buildTree();

        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(tree.getPartition("root"), tree.getPartition("child1"), tree.getPartition("child2"));

        // When / Then
        //  - Attempting to split something that has already been split should fail
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(
                        tree.getPartition("root"), tree.getPartition("child1"), tree.getPartition("child2")))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldThrowExceptionWithPartitionSplitRequestWhereChildrenWrong() throws Exception {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        Partition parentPartitionAfterSplit = parentPartition.toBuilder()
                .leafPartition(false)
                .childPartitionIds(Arrays.asList("child3", "child2")) // Wrong children
                .build();
        Region region1 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId(parentPartition.getId())
                .build();
        Region region2 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId(parentPartition.getId())
                .build();

        // When / Then
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(
                        parentPartitionAfterSplit, childPartition1, childPartition2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldThrowExceptionWithPartitionSplitRequestWhereParentWrong() throws Exception {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        Partition parentPartitionAfterSplit = parentPartition.toBuilder()
                .leafPartition(false)
                .childPartitionIds(Arrays.asList("child1", "child2"))
                .build();
        Region region1 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("notparent") // Wrong parent
                .build();
        Region region2 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();

        // When / Then
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(
                        parentPartitionAfterSplit, childPartition1, childPartition2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldThrowExceptionWithPartitionSplitRequestWhereNewPartitionIsNotLeaf() throws Exception {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        Partition parentPartitionAfterSplit = parentPartition.toBuilder()
                .leafPartition(false)
                .childPartitionIds(Arrays.asList("child1", "child2"))
                .build();
        Region region1 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, 0L));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();
        Region region2 = new Region(new RangeFactory(schema).createRange(field, 0L, Long.MAX_VALUE));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(false) // Not leaf
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();

        // When / Then
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(
                        parentPartitionAfterSplit, childPartition1, childPartition2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForIntKey() throws Exception {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertThat(partitions).hasSize(1);
        Partition expectedPartition = new PartitionsBuilder(schema)
                .rootFirst(partitions.get(0).getId())
                .buildTree()
                .getPartition(partitions.get(0).getId());
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForLongKey() throws Exception {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertThat(partitions).hasSize(1);
        Partition expectedPartition = new PartitionsBuilder(schema)
                .rootFirst(partitions.get(0).getId())
                .buildTree()
                .getPartition(partitions.get(0).getId());
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForStringKey() throws Exception {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertThat(partitions).hasSize(1);
        Partition expectedPartition = new PartitionsBuilder(schema)
                .rootFirst(partitions.get(0).getId())
                .buildTree()
                .getPartition(partitions.get(0).getId());
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForByteArrayKey() throws Exception {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertThat(partitions).hasSize(1);
        Partition expectedPartition = new PartitionsBuilder(schema)
                .rootFirst(partitions.get(0).getId())
                .buildTree()
                .getPartition(partitions.get(0).getId());
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    void shouldNotReinitialisePartitionsWhenAFileIsPresent() throws Exception {
        // Given
        Schema schema = schemaWithKey("key", new LongType());
        PartitionTree treeBefore = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "before1", "before2", 0L)
                .buildTree();
        PartitionTree treeAfter = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "after1", "after2", 10L)
                .buildTree();
        StateStore stateStore = getStateStore(schema, treeBefore.getAllPartitions());
        stateStore.addFile(FileInfoFactory.from(treeBefore).partitionFile("before2", 100L));

        // When / Then
        assertThatThrownBy(() -> stateStore.initialise(treeAfter.getAllPartitions()))
                .isInstanceOf(StateStoreException.class);
        assertThat(stateStore.getAllPartitions())
                .containsExactlyInAnyOrderElementsOf(treeBefore.getAllPartitions());
    }

    @Test
    void shouldReinitialisePartitionsWhenNoFilesArePresent() throws Exception {
        // Given
        Schema schema = schemaWithKey("key", new LongType());
        PartitionTree treeBefore = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "before1", "before2", 0L)
                .buildTree();
        PartitionTree treeAfter = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "after1", "after2", 10L)
                .buildTree();
        StateStore stateStore = getStateStore(schema, treeBefore.getAllPartitions());

        // When
        stateStore.initialise(treeAfter.getAllPartitions());

        // Then
        assertThat(stateStore.getAllPartitions())
                .containsExactlyInAnyOrderElementsOf(treeAfter.getAllPartitions());
    }

    private S3StateStore getStateStore(Schema schema,
                                       List<Partition> partitions,
                                       int garbageCollectorDelayBeforeDeletionInMinutes) throws StateStoreException {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, garbageCollectorDelayBeforeDeletionInMinutes);
        S3StateStore stateStore = new S3StateStore(instanceProperties, tableProperties, dynamoDBClient, new Configuration());
        stateStore.initialise(partitions);
        return stateStore;
    }

    private S3StateStore getStateStore(Schema schema,
                                       List<Partition> partitions) throws StateStoreException {
        return getStateStore(schema, partitions, 0);
    }

    private S3StateStore getStateStoreFromSplitPoints(Schema schema, List<Object> splitPoints) throws StateStoreException {
        return getStateStore(schema, new PartitionsFromSplitPoints(schema, splitPoints).construct(), 0);
    }

    private S3StateStore getStateStore(Schema schema, int garbageCollectorDelayBeforeDeletionInMinutes) throws StateStoreException {
        return getStateStore(schema, new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct(), garbageCollectorDelayBeforeDeletionInMinutes);
    }

    private S3StateStore getStateStore(Schema schema) throws StateStoreException {
        return getStateStoreFromSplitPoints(schema, Collections.emptyList());
    }

    private Schema schemaWithSingleRowKeyType(PrimitiveType type) {
        return Schema.builder().rowKeyFields(new Field("key", type)).build();
    }

    private Schema schemaWithTwoRowKeyTypes(PrimitiveType type1, PrimitiveType type2) {
        return Schema.builder().rowKeyFields(new Field("key1", type1), new Field("key2", type2)).build();
    }

    private Schema schemaWithKeyAndValueWithTypes(PrimitiveType keyType, Type valueType) {
        return Schema.builder()
                .rowKeyFields(new Field("key", keyType))
                .valueFields(new Field("value", valueType))
                .build();
    }
}
