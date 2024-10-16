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
package sleeper.ingest.impl;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriter;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriterAcceptingRecords;
import sleeper.ingest.testutils.IngestCoordinatorTestParameters;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.ingest.testutils.ResultVerifier;
import sleeper.ingest.testutils.TestFilesAndRecords;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_BATCH_BUFFER_BYTES;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_MAX_LOCAL_STORE_BYTES;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_WORKING_BUFFER_BYTES;
import static sleeper.core.statestore.testutils.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;

class IngestCoordinatorUsingDirectWriteBackedByArrowRecordWriterAcceptingRecordListIT extends DirectWriteBackedByArrowTestBase {

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
        stateStore.fixFileUpdateTime(stateStoreUpdateTime);
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(recordListAndSchema, parameters, properties -> {
            properties.setNumber(ARROW_INGEST_WORKING_BUFFER_BYTES, 16 * 1024 * 1024L);
            properties.setNumber(ARROW_INGEST_BATCH_BUFFER_BYTES, 16 * 1024 * 1024L);
            properties.setNumber(ARROW_INGEST_MAX_LOCAL_STORE_BYTES, 128 * 1024 * 1024L);
        });

        // Then
        TestFilesAndRecords actualActiveData = TestFilesAndRecords.loadActiveFiles(stateStore, recordListAndSchema.sleeperSchema, configuration);

        assertThat(actualActiveData.getFiles())
                .extracting(FileReference::getPartitionId, FileReference::getFilename)
                .containsExactlyInAnyOrder(
                        tuple("left", parameters.getLocalFilePrefix() + "/data/partition_left/leftFile.parquet"),
                        tuple("right", parameters.getLocalFilePrefix() + "/data/partition_right/rightFile.parquet"));

        assertThat(actualActiveData.getSetOfAllRecords())
                .isEqualTo(new HashSet<>(recordListAndSchema.recordList));
        assertThat(actualActiveData.getPartitionData("left").streamAllRecords())
                .extracting(record -> record.get("key0"))
                .containsExactlyElementsOf(LongStream.range(-10000, 0).boxed()
                        .collect(Collectors.toList()));
        assertThat(actualActiveData.getPartitionData("right").streamAllRecords())
                .extracting(record -> record.get("key0"))
                .containsExactlyElementsOf(LongStream.range(0, 10000).boxed()
                        .collect(Collectors.toList()));

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getField("key0").orElseThrow(),
                recordListAndSchema,
                actualActiveData.getFiles(),
                configuration);
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
        stateStore.fixFileUpdateTime(stateStoreUpdateTime);
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile1", "rightFile1", "leftFile2", "rightFile2"))
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(recordListAndSchema, parameters, properties -> {
            properties.setNumber(ARROW_INGEST_WORKING_BUFFER_BYTES, 16 * 1024 * 1024L);
            properties.setNumber(ARROW_INGEST_BATCH_BUFFER_BYTES, 16 * 1024 * 1024L);
            properties.setNumber(ARROW_INGEST_MAX_LOCAL_STORE_BYTES, 2 * 1024 * 1024L);
        });

        // Then
        TestFilesAndRecords actualActiveData = TestFilesAndRecords.loadActiveFiles(stateStore, recordListAndSchema.sleeperSchema, configuration);

        assertThat(actualActiveData.getFiles())
                .extracting(FileReference::getPartitionId, FileReference::getFilename)
                .containsExactlyInAnyOrder(
                        tuple("left", parameters.getLocalFilePrefix() + "/data/partition_left/leftFile1.parquet"),
                        tuple("left", parameters.getLocalFilePrefix() + "/data/partition_left/leftFile2.parquet"),
                        tuple("right", parameters.getLocalFilePrefix() + "/data/partition_right/rightFile1.parquet"),
                        tuple("right", parameters.getLocalFilePrefix() + "/data/partition_right/rightFile2.parquet"));
        assertThat(actualActiveData.getSetOfAllRecords())
                .isEqualTo(new HashSet<>(recordListAndSchema.recordList));
        assertThat(actualActiveData.getPartitionData("left"))
                .satisfies(data -> assertThat(data.getFiles()).allSatisfy(
                        file -> assertThatRecordsHaveFieldValuesThatAllAppearInRangeInSameOrder(
                                data.getRecordsInFile(file),
                                "key0", LongStream.range(-10_000, 0))))
                .satisfies(data -> assertThat(data.getNumRecords()).isEqualTo(10_000));
        assertThat(actualActiveData.getPartitionData("right"))
                .satisfies(data -> assertThat(data.getFiles()).allSatisfy(
                        file -> assertThatRecordsHaveFieldValuesThatAllAppearInRangeInSameOrder(
                                data.getRecordsInFile(file),
                                "key0", LongStream.range(0, 10_000))))
                .satisfies(data -> assertThat(data.getNumRecords()).isEqualTo(10_000));

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getField("key0").orElseThrow(),
                recordListAndSchema,
                actualActiveData.getFiles(),
                configuration);
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
                        .buildList());
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString();
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        stateStore.fixFileUpdateTime(stateStoreUpdateTime);
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        assertThatThrownBy(() -> ingestRecords(recordListAndSchema, parameters, properties -> {
            properties.setNumber(ARROW_INGEST_WORKING_BUFFER_BYTES, 32 * 1024L);
            properties.setNumber(ARROW_INGEST_BATCH_BUFFER_BYTES, 32 * 1024L);
            properties.setNumber(ARROW_INGEST_MAX_LOCAL_STORE_BYTES, 64 * 1024 * 1024L);
        })).isInstanceOf(OutOfMemoryException.class).hasNoSuppressedExceptions();
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

    private static void ingestRecords(
            RecordGenerator.RecordListAndSchema recordListAndSchema, IngestCoordinatorTestParameters parameters,
            Consumer<InstanceProperties> config) throws Exception {
        try (IngestCoordinator<RecordList> ingestCoordinator = parameters
                .toBuilder().localDirectWrite().setInstanceProperties(config).build()
                .buildCoordinatorWithArrowWriter(new ArrowRecordWriterAcceptingRecordList())) {
            for (RecordList recordList : buildScrambledRecordLists(recordListAndSchema)) {
                ingestCoordinator.write(recordList);
            }
        }
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
