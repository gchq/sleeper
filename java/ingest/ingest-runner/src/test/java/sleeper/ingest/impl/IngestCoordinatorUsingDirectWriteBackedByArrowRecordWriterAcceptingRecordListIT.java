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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriter;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriterAcceptingRecords;
import sleeper.ingest.testutils.IngestCoordinatorTestParameters;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.ingest.testutils.ResultVerifier;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;
import static sleeper.ingest.testutils.IngestCoordinatorFactory.ingestCoordinatorDirectWriteBackedByArrow;
import static sleeper.ingest.testutils.ResultVerifier.readMergedRecordsFromPartitionDataFiles;

class IngestCoordinatorUsingDirectWriteBackedByArrowRecordWriterAcceptingRecordListIT {
    @TempDir
    public Path temporaryFolder;
    private final Configuration configuration = new Configuration();

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
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .fileUpdatedTimes(() -> stateStoreUpdateTime)
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(recordListAndSchema, parameters, arrowConfig -> arrowConfig
                .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                .batchBufferAllocatorBytes(16 * 1024 * 1024L)
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

        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(
                recordListAndSchema.sleeperSchema, actualFiles, new Configuration());

        assertThat(actualFiles).containsExactlyInAnyOrder(leftFile, rightFile);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(actualRecords).extracting(record -> record.getValues(List.of("key0")))
                .containsExactlyInAnyOrderElementsOf(LongStream.range(-10000, 10000).boxed()
                        .map(List::<Object>of)
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
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile1", "rightFile1", "leftFile2", "rightFile2"))
                .fileUpdatedTimes(() -> stateStoreUpdateTime)
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(recordListAndSchema, parameters, arrowConfig -> arrowConfig
                .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                .batchBufferAllocatorBytes(16 * 1024 * 1024L)
                .maxNoOfBytesToWriteLocally(2 * 1024 * 1024L));

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        FileInfo leftFile1 = fileInfoFactory.leafFile(parameters.getLocalFilePrefix() +
                "/partition_left/leftFile1.parquet", 5950, -9999L, -1L);
        FileInfo leftFile2 = fileInfoFactory.leafFile(parameters.getLocalFilePrefix() +
                "/partition_left/leftFile2.parquet", 4050, -10000L, -3L);
        FileInfo rightFile1 = fileInfoFactory.leafFile(parameters.getLocalFilePrefix() +
                "/partition_right/rightFile1.parquet", 6050, 1L, 9998L);
        FileInfo rightFile2 = fileInfoFactory.leafFile(parameters.getLocalFilePrefix() +
                "/partition_right/rightFile2.parquet", 3950, 0L, 9999L);

        List<Record> leftFile1Records = readMergedRecordsFromPartitionDataFiles(
                recordListAndSchema.sleeperSchema, List.of(leftFile1), configuration);
        List<Record> leftFile2Records = readMergedRecordsFromPartitionDataFiles(
                recordListAndSchema.sleeperSchema, List.of(leftFile2), configuration);
        List<Record> rightFile1Records = readMergedRecordsFromPartitionDataFiles(
                recordListAndSchema.sleeperSchema, List.of(rightFile1), configuration);
        List<Record> rightFile2Records = readMergedRecordsFromPartitionDataFiles(
                recordListAndSchema.sleeperSchema, List.of(rightFile2), configuration);
        List<Record> actualRecords = Stream.of(leftFile1Records, leftFile2Records, rightFile1Records, rightFile2Records)
                .flatMap(List::stream)
                .collect(Collectors.toUnmodifiableList());

        assertThat(actualFiles).containsExactlyInAnyOrder(leftFile1, rightFile1, leftFile2, rightFile2);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThatOrderedRecordsInRange(leftFile1Records, LongStream.range(-9999L, 0));
        assertThatOrderedRecordsInRange(leftFile2Records, LongStream.range(-10000L, -2));
        assertThatOrderedRecordsInRange(rightFile1Records, LongStream.range(1L, 9999));
        assertThatOrderedRecordsInRange(rightFile2Records, LongStream.range(0, 10000));

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getField("key0").orElseThrow(),
                recordListAndSchema,
                actualFiles,
                configuration
        );
    }

    @Test
    void shouldErrorWhenBatchBufferAndWorkingBufferAreSmall() throws IOException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        StateStore stateStore = inMemoryStateStoreWithFixedPartitions(
                new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                        .rootFirst("root")
                        .splitToNewChildren("root", "left", "right", 0L)
                        .buildList()
        );
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString();
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .fileUpdatedTimes(() -> stateStoreUpdateTime)
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        assertThatThrownBy(() -> ingestRecords(recordListAndSchema, parameters, arrowConfig -> arrowConfig
                .workingBufferAllocatorBytes(32 * 1024L)
                .batchBufferAllocatorBytes(32 * 1024L)
                .maxNoOfBytesToWriteLocally(64 * 1024 * 1024L)))
                .isInstanceOf(OutOfMemoryException.class)
                .hasNoSuppressedExceptions();
    }

    private static List<RecordList> buildScrambledRecordLists(RecordGenerator.RecordListAndSchema recordListAndSchema) {
        RecordList[] recordLists = new RecordList[5];
        for (int i = 0; i < recordLists.length; i++) {
            recordLists[i] = new RecordList();
        }
        int i = 0;
        for (Record record : recordListAndSchema.recordList) {
            recordLists[i].addRecord(record);
            i++;
            if (i == 5) {
                i = 0;
            }
        }
        return List.of(recordLists);
    }

    private IngestCoordinatorTestParameters.Builder createTestParameterBuilder() {
        return IngestCoordinatorTestParameters
                .builder()
                .temporaryFolder(temporaryFolder)
                .hadoopConfiguration(configuration);
    }

    private static void ingestRecords(RecordGenerator.RecordListAndSchema recordListAndSchema,
                                      IngestCoordinatorTestParameters parameters,
                                      Consumer<ArrowRecordBatchFactory.Builder<RecordList>> arrowConfig) throws Exception {
        try (IngestCoordinator<RecordList> ingestCoordinator = createIngestCoordinator(parameters, arrowConfig)) {
            for (RecordList recordList : buildScrambledRecordLists(recordListAndSchema)) {
                ingestCoordinator.write(recordList);
            }
        }
    }

    private static IngestCoordinator<RecordList> createIngestCoordinator(IngestCoordinatorTestParameters parameters,
                                                                         Consumer<ArrowRecordBatchFactory.Builder<RecordList>> arrowConfig) {
        return ingestCoordinatorDirectWriteBackedByArrow(parameters, parameters.getLocalFilePrefix(),
                arrowConfig, new ArrowRecordWriterAcceptingRecordList());
    }

    private static void assertThatOrderedRecordsInRange(List<Record> records, LongStream range) {
        assertThat(range.boxed()
                .map(List::<Object>of)
                .collect(Collectors.toList()))
                .containsSubsequence(records.stream()
                        .map(record -> record.getValues(List.of("key0")))
                        .collect(Collectors.toList()));
    }

    static class RecordList {
        private final List<Record> records;

        RecordList() {
            this.records = new ArrayList<>();
        }

        public void addRecord(Record record) {
            records.add(record);
        }

        public List<Record> getRecords() {
            return records;
        }
    }

    static class ArrowRecordWriterAcceptingRecordList implements ArrowRecordWriter<RecordList> {

        @Override
        public int insert(List<Field> allFields, VectorSchemaRoot vectorSchemaRoot, RecordList recordList, int startInsertAtRowNo) {
            int i = 0;
            for (Record record : recordList.getRecords()) {
                ArrowRecordWriterAcceptingRecords.writeRecord(
                        allFields, vectorSchemaRoot, record, startInsertAtRowNo + i);
                i++;
            }
            int finalRowCount = startInsertAtRowNo + i;
            vectorSchemaRoot.setRowCount(finalRowCount);
            return finalRowCount;
        }
    }
}
