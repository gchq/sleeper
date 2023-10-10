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
package sleeper.ingest.impl;

import org.apache.arrow.memory.OutOfMemoryException;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.ingest.testutils.IngestCoordinatorTestParameters;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.ingest.testutils.ResultVerifier;
import sleeper.ingest.testutils.TestIngestType;

import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;
import static sleeper.ingest.testutils.ResultVerifier.readRecordsFromPartitionDataFile;
import static sleeper.ingest.testutils.TestIngestType.directWriteBackedByArrowWriteToLocalFile;

class IngestCoordinatorUsingDirectWriteBackedByArrowIT extends DirectWriteBackedByArrowTestBase {
    @Test
    void shouldWriteRecordsWhenThereAreMoreRecordsInAPartitionThanCanFitInMemory() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 0L)
                .buildTree();
        StateStore stateStore = inMemoryStateStoreWithFixedPartitions(tree.getAllPartitions());
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString();
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        stateStore.fixTime(stateStoreUpdateTime);
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(recordListAndSchema, parameters, arrowConfig -> arrowConfig
                .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                .batchBufferAllocatorBytes(4 * 1024 * 1024L)
                .maxNoOfBytesToWriteLocally(128 * 1024 * 1024L));

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        FileInfo leftFile = fileInfoFactory.leafFile(parameters.getLocalFilePrefix() +
                "/partition_left/leftFile.parquet", 10000, -10000L, -1L);
        FileInfo rightFile = fileInfoFactory.leafFile(parameters.getLocalFilePrefix() +
                "/partition_right/rightFile.parquet", 10000, 0L, 9999L);

        List<Record> leftFileRecords = readRecordsFromPartitionDataFile(
                recordListAndSchema.sleeperSchema, leftFile, configuration);
        List<Record> rightFileRecords = readRecordsFromPartitionDataFile(
                recordListAndSchema.sleeperSchema, rightFile, configuration);
        List<Record> actualRecords = Stream.of(leftFileRecords, rightFileRecords)
                .flatMap(List::stream)
                .collect(Collectors.toUnmodifiableList());

        assertThat(actualFiles).containsExactlyInAnyOrder(leftFile, rightFile);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(leftFileRecords).extracting(record -> record.get("key0"))
                .containsExactlyElementsOf(LongStream.range(-10000, 0).boxed()
                        .collect(Collectors.toList()));
        assertThat(rightFileRecords).extracting(record -> record.get("key0"))
                .containsExactlyElementsOf(LongStream.range(0, 10000).boxed()
                        .collect(Collectors.toList()));

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getField("key0").orElseThrow(),
                recordListAndSchema,
                actualFiles,
                configuration
        );
    }

    @Test
    void shouldWriteRecordsWhenThereAreMoreRecordsThanCanFitInLocalFile() throws Exception {
        // Given
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 0L)
                .buildTree();
        StateStore stateStore = inMemoryStateStoreWithFixedPartitions(tree.getAllPartitions());
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString();
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        stateStore.fixTime(stateStoreUpdateTime);
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile1", "rightFile1", "leftFile2", "rightFile2"))
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(recordListAndSchema, parameters, arrowConfig -> arrowConfig
                .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                .batchBufferAllocatorBytes(4 * 1024 * 1024L)
                .maxNoOfBytesToWriteLocally(16 * 1024 * 1024L));

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        FileInfo leftFile1 = fileInfoFactory.partitionFile("left",
                parameters.getLocalFilePrefix() + "/partition_left/leftFile1.parquet", 7908);
        FileInfo leftFile2 = fileInfoFactory.partitionFile("left",
                parameters.getLocalFilePrefix() + "/partition_left/leftFile2.parquet", 2092);
        FileInfo rightFile1 = fileInfoFactory.partitionFile("right",
                parameters.getLocalFilePrefix() + "/partition_right/rightFile1.parquet", 7791);
        FileInfo rightFile2 = fileInfoFactory.partitionFile("right",
                parameters.getLocalFilePrefix() + "/partition_right/rightFile2.parquet", 2209);

        List<Record> leftFile1Records = readRecordsFromPartitionDataFile(
                recordListAndSchema.sleeperSchema, leftFile1, configuration);
        List<Record> leftFile2Records = readRecordsFromPartitionDataFile(
                recordListAndSchema.sleeperSchema, leftFile2, configuration);
        List<Record> rightFile1Records = readRecordsFromPartitionDataFile(
                recordListAndSchema.sleeperSchema, rightFile1, configuration);
        List<Record> rightFile2Records = readRecordsFromPartitionDataFile(
                recordListAndSchema.sleeperSchema, rightFile2, configuration);
        List<Record> actualRecords = Stream.of(leftFile1Records, leftFile2Records, rightFile1Records, rightFile2Records)
                .flatMap(List::stream)
                .collect(Collectors.toUnmodifiableList());

        assertThat(actualFiles).containsExactlyInAnyOrder(leftFile1, leftFile2, rightFile1, rightFile2);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThatRecordsHaveFieldValuesThatAllAppearInRangeInSameOrder(leftFile1Records,
                "key0", LongStream.range(-10000, -1));
        assertThatRecordsHaveFieldValuesThatAllAppearInRangeInSameOrder(leftFile2Records,
                "key0", LongStream.range(-9998L, 0));
        assertThatRecordsHaveFieldValuesThatAllAppearInRangeInSameOrder(rightFile1Records,
                "key0", LongStream.range(1L, 9999));
        assertThatRecordsHaveFieldValuesThatAllAppearInRangeInSameOrder(rightFile2Records,
                "key0", LongStream.range(0, 10000));

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getField("key0").orElseThrow(),
                recordListAndSchema,
                actualFiles,
                configuration
        );
    }

    @Test
    void shouldErrorWhenBatchBufferAndWorkingBufferAreSmall() throws Exception {
        // Given
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        StateStore stateStore = inMemoryStateStoreWithFixedPartitions(
                new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                        .rootFirst("root")
                        .splitToNewChildren("root", "left", "right", 0L).buildList());
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString();
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        stateStore.fixTime(stateStoreUpdateTime);
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When/Then
        assertThatThrownBy(() -> ingestRecords(recordListAndSchema, parameters, arrowConfig -> arrowConfig
                .workingBufferAllocatorBytes(32 * 1024L)
                .batchBufferAllocatorBytes(32 * 1024L)
                .maxNoOfBytesToWriteLocally(64 * 1024 * 1024L)))
                .isInstanceOf(OutOfMemoryException.class)
                .hasNoSuppressedExceptions();
    }

    private static void ingestRecords(RecordGenerator.RecordListAndSchema recordListAndSchema,
                                      IngestCoordinatorTestParameters parameters,
                                      Consumer<ArrowRecordBatchFactory.Builder<Record>> arrowConfig) throws Exception {
        TestIngestType ingestType = directWriteBackedByArrowWriteToLocalFile(arrowConfig);
        try (IngestCoordinator<Record> ingestCoordinator = ingestType.createIngestCoordinator(parameters)) {
            for (Record record : recordListAndSchema.recordList) {
                ingestCoordinator.write(record);
            }
        }
    }
}
