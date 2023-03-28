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

import sleeper.core.iterator.IteratorException;
import sleeper.core.key.Key;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriter;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriterAcceptingRecords;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.ingest.testutils.ResultVerifier;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.inmemory.StateStoreTestBuilder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.parquetConfiguration;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.standardIngestCoordinator;

class IngestCoordinatorUsingDirectWriteBackedByArrowRecordWriterAcceptingRecordListIT {
    @TempDir
    public Path temporaryFolder;

    @Test
    void shouldWriteRecordsWhenThereAreMoreRecordsInAPartitionThanCanFitInMemory() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        StateStore stateStore = buildStateStoreWithSingleSplitPoint(recordListAndSchema, 0L);
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 0L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerifyUsingDirectWriteBackedByArrow(
                recordListAndSchema,
                stateStore,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                16 * 1024 * 1024L,
                16 * 1024 * 1024L,
                128 * 1024 * 1024L);
    }

    @Test
    void shouldWriteRecordsWhenThereAreMoreRecordsThanCanFitInLocalFile() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        StateStore stateStore = buildStateStoreWithSingleSplitPoint(recordListAndSchema, 0L);
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 0L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 2),
                        new AbstractMap.SimpleEntry<>(1, 2))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerifyUsingDirectWriteBackedByArrow(
                recordListAndSchema,
                stateStore,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                16 * 1024 * 1024L,
                16 * 1024 * 1024L,
                2 * 1024 * 1024L);
    }

    @Test
    void shouldErrorWhenBatchBufferAndWorkingBufferAreSmall() {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        StateStore stateStore = buildStateStoreWithSingleSplitPoint(recordListAndSchema, 0L);
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 0L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 2),
                        new AbstractMap.SimpleEntry<>(1, 2))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertThatThrownBy(() ->
                ingestAndVerifyUsingDirectWriteBackedByArrow(
                        recordListAndSchema,
                        stateStore,
                        keyToPartitionNoMappingFn,
                        partitionNoToExpectedNoOfFilesMap,
                        32 * 1024L,
                        32 * 1024L,
                        64 * 1024 * 1024L))
                .isInstanceOf(OutOfMemoryException.class)
                .hasNoSuppressedExceptions();
    }

    private StateStore buildStateStoreWithSingleSplitPoint(
            RecordGenerator.RecordListAndSchema recordListAndSchema, Object splitPoint) {
        return StateStoreTestBuilder.from(new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                        .treeWithSingleSplitPoint(splitPoint))
                .buildStateStore();
    }

    private void ingestAndVerifyUsingDirectWriteBackedByArrow(
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            StateStore stateStore,
            Function<Key, Integer> keyToPartitionNoMappingFn,
            Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap,
            long arrowWorkingBytes,
            long arrowBatchBytes,
            long localStoreBytes) throws IOException, StateStoreException, IteratorException {
        String localWorkingDirectory = createTempDirectory(temporaryFolder, null).toString();
        Configuration hadoopConfiguration = new Configuration();

        try (IngestCoordinator<RecordList> ingestCoordinator = buildIngestCoordinator(
                stateStore, recordListAndSchema.sleeperSchema, hadoopConfiguration,
                localWorkingDirectory, arrowWorkingBytes, arrowBatchBytes, localStoreBytes)) {

            for (RecordList recordList : buildScrambledRecordLists(recordListAndSchema)) {
                ingestCoordinator.write(recordList);
            }
        }

        ResultVerifier.verify(
                stateStore,
                recordListAndSchema.sleeperSchema,
                keyToPartitionNoMappingFn,
                recordListAndSchema.recordList,
                partitionNoToExpectedNoOfFilesMap,
                hadoopConfiguration,
                localWorkingDirectory);
    }

    private IngestCoordinator<RecordList> buildIngestCoordinator(
            StateStore stateStore, Schema schema, Configuration hadoopConfiguration,
            String localWorkingDirectory,
            long arrowWorkingBytes,
            long arrowBatchBytes,
            long localStoreBytes) throws IOException {
        String ingestToDirectory = createTempDirectory(temporaryFolder, null).toString();
        ParquetConfiguration parquetConfiguration = parquetConfiguration(schema, hadoopConfiguration);
        return standardIngestCoordinator(
                stateStore, schema,
                ArrowRecordBatchFactory.builder()
                        .schema(schema)
                        .maxNoOfRecordsToWriteToArrowFileAtOnce(128)
                        .workingBufferAllocatorBytes(arrowWorkingBytes)
                        .minBatchBufferAllocatorBytes(arrowBatchBytes)
                        .maxBatchBufferAllocatorBytes(arrowBatchBytes)
                        .maxNoOfBytesToWriteLocally(localStoreBytes)
                        .localWorkingDirectory(localWorkingDirectory)
                        .recordWriter(new ArrowRecordWriterAcceptingRecordList())
                        .build(),
                DirectPartitionFileWriterFactory.from(
                        parquetConfiguration, ingestToDirectory));
    }

    private RecordList[] buildScrambledRecordLists(RecordGenerator.RecordListAndSchema recordListAndSchema) {
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
        return recordLists;
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
