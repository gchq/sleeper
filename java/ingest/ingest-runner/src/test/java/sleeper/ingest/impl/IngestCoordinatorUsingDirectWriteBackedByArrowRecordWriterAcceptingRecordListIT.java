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
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.key.Key;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.inmemory.StateStoreTestBuilder;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriter;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriterAcceptingRecords;
import sleeper.ingest.testutils.RecordGenerator;

import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.parquetConfiguration;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.standardIngestCoordinator;
import static sleeper.ingest.testutils.ResultVerifier.assertOnSketch;
import static sleeper.ingest.testutils.ResultVerifier.createFieldToItemSketchMap;
import static sleeper.ingest.testutils.ResultVerifier.readFieldToItemSketchMap;
import static sleeper.ingest.testutils.ResultVerifier.readMergedRecordsFromPartitionDataFiles;

class IngestCoordinatorUsingDirectWriteBackedByArrowRecordWriterAcceptingRecordListIT {
    @TempDir
    public Path temporaryFolder;

    @Test
    void shouldWriteRecordsWhenThereAreMoreRecordsInAPartitionThanCanFitInMemory() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 0L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerify(recordListAndSchema, arrow -> arrow
                        .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                        .batchBufferAllocatorBytes(16 * 1024 * 1024L)
                        .maxNoOfBytesToWriteLocally(128 * 1024 * 1024L),
                keyToPartitionNoMappingFn, partitionNoToExpectedNoOfFilesMap);
    }

    @Test
    void shouldWriteRecordsWhenThereAreMoreRecordsThanCanFitInLocalFile() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 0L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 2),
                        new AbstractMap.SimpleEntry<>(1, 2))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerify(recordListAndSchema, arrow -> arrow
                        .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                        .batchBufferAllocatorBytes(16 * 1024 * 1024L)
                        .maxNoOfBytesToWriteLocally(2 * 1024 * 1024L),
                keyToPartitionNoMappingFn, partitionNoToExpectedNoOfFilesMap);
    }

    @Test
    void shouldErrorWhenBatchBufferAndWorkingBufferAreSmall() {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 0L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 2),
                        new AbstractMap.SimpleEntry<>(1, 2))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertThatThrownBy(() ->
                ingestAndVerify(recordListAndSchema, arrow -> arrow
                                .workingBufferAllocatorBytes(32 * 1024L)
                                .batchBufferAllocatorBytes(32 * 1024L)
                                .maxNoOfBytesToWriteLocally(64 * 1024 * 1024L),
                        keyToPartitionNoMappingFn, partitionNoToExpectedNoOfFilesMap))
                .isInstanceOf(OutOfMemoryException.class)
                .hasNoSuppressedExceptions();
    }

    private void ingestAndVerify(RecordGenerator.RecordListAndSchema recordListAndSchema,
                                 Consumer<ArrowRecordBatchFactory.Builder<RecordList>> arrowConfig,
                                 Function<Key, Integer> keyToPartitionNoMappingFn,
                                 Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap) throws Exception {
        Schema schema = recordListAndSchema.sleeperSchema;
        PartitionsBuilder partitions = new PartitionsBuilder(schema).treeWithSingleSplitPoint(0L);
        StateStore stateStore = StateStoreTestBuilder.from(partitions).buildStateStore();
        ParquetConfiguration parquetConfiguration = parquetConfiguration(schema, new Configuration());

        String localDirectory = createTempDirectory(temporaryFolder, null).toString();
        String ingestToDirectory = createTempDirectory(temporaryFolder, null).toString();
        PartitionFileWriterFactory partitionFileWriterFactory = DirectPartitionFileWriterFactory.from(
                parquetConfiguration, ingestToDirectory);
        ArrowRecordBatchFactory<RecordList> arrowRecordBatchFactory = createArrowRecordBatchFactory(
                localDirectory, schema, arrowConfig);

        try (IngestCoordinator<RecordList> ingestCoordinator = standardIngestCoordinator(
                stateStore, schema, arrowRecordBatchFactory, partitionFileWriterFactory)) {
            for (RecordList write : buildScrambledRecordLists(recordListAndSchema)) {
                ingestCoordinator.write(write);
            }
        }
        verify(localDirectory, schema, stateStore, recordListAndSchema.recordList,
                keyToPartitionNoMappingFn, partitionNoToExpectedNoOfFilesMap);
    }

    private void verify(String localWorkingDirectory, Schema schema, StateStore stateStore, List<Record> expectedRecords,
                        Function<Key, Integer> keyToPartitionNoMappingFn,
                        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap) throws StateStoreException {
        assertThat(Path.of(localWorkingDirectory)).isEmptyDirectory();

        PartitionTree partitionTree = new PartitionTree(schema, stateStore.getAllPartitions());

        Map<Integer, List<Record>> partitionNoToExpectedRecordsMap = expectedRecords.stream()
                .collect(Collectors.groupingBy(
                        record -> keyToPartitionNoMappingFn.apply(Key.create(record.getValues(schema.getRowKeyFieldNames())))));

        Map<String, List<FileInfo>> partitionIdToFileInfosMap = stateStore.getActiveFiles().stream()
                .collect(Collectors.groupingBy(FileInfo::getPartitionId));

        Map<String, Integer> partitionIdToPartitionNoMap = partitionNoToExpectedRecordsMap.entrySet().stream()
                .map(entry -> {
                    Key keyOfFirstRecord = Key.create(entry.getValue().get(0).getValues(schema.getRowKeyFieldNames()));
                    return new AbstractMap.SimpleEntry<>(partitionTree.getLeafPartition(keyOfFirstRecord).getId(), entry.getKey());
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<Integer, List<FileInfo>> partitionNoToFileInfosMap = partitionIdToFileInfosMap.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> partitionIdToPartitionNoMap.get(entry.getKey()),
                        Map.Entry::getValue));

        int expectedTotalNoOfFiles = partitionNoToExpectedNoOfFilesMap.values().stream()
                .mapToInt(Integer::valueOf)
                .sum();

        Set<Integer> allPartitionNoSet = Stream.of(
                        partitionNoToFileInfosMap.keySet().stream(),
                        partitionNoToExpectedNoOfFilesMap.keySet().stream(),
                        partitionNoToExpectedRecordsMap.keySet().stream())
                .flatMap(Function.identity())
                .collect(Collectors.toSet());

        assertThat(stateStore.getActiveFiles()).hasSize(expectedTotalNoOfFiles);
        assertThat(allPartitionNoSet).allMatch(partitionNoToExpectedNoOfFilesMap::containsKey);

        allPartitionNoSet.forEach(partitionNo -> verifyPartition(schema, new Configuration(),
                partitionNoToFileInfosMap.getOrDefault(partitionNo, Collections.emptyList()),
                partitionNoToExpectedNoOfFilesMap.get(partitionNo),
                partitionNoToExpectedRecordsMap.getOrDefault(partitionNo, Collections.emptyList())));
    }

    private void verifyPartition(Schema schema, Configuration hadoopConfiguration, List<FileInfo> partitionFileInfoList,
                                 int expectedNoOfFiles, List<Record> expectedRecords) {
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(schema, partitionFileInfoList, hadoopConfiguration);

        assertThat(partitionFileInfoList).hasSize(expectedNoOfFiles);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);

        // In some situations, check that the file min and max match the min and max of dimension 0
        if (expectedNoOfFiles == 1 &&
                schema.getRowKeyFields().get(0).getType() instanceof LongType) {
            String rowKeyFieldNameDimension0 = schema.getRowKeyFieldNames().get(0);
            Key minRowKeyDimension0 = expectedRecords.stream()
                    .map(record -> (Long) record.get(rowKeyFieldNameDimension0))
                    .min(Comparator.naturalOrder())
                    .map(Key::create)
                    .orElseThrow();
            Key maxRowKeyDimension0 = expectedRecords.stream()
                    .map(record -> (Long) record.get(rowKeyFieldNameDimension0))
                    .max(Comparator.naturalOrder())
                    .map(Key::create)
                    .orElseThrow();
            partitionFileInfoList.forEach(fileInfo -> {
                assertThat(fileInfo.getMinRowKey()).isEqualTo(minRowKeyDimension0);
                assertThat(fileInfo.getMaxRowKey()).isEqualTo(maxRowKeyDimension0);
            });
        }

        if (expectedNoOfFiles > 0) {
            Map<Field, ItemsSketch> expectedFieldToItemsSketchMap = createFieldToItemSketchMap(schema, expectedRecords);
            Map<Field, ItemsSketch> savedFieldToItemsSketchMap = readFieldToItemSketchMap(schema, partitionFileInfoList, hadoopConfiguration);
            schema.getRowKeyFields().forEach(field ->
                    assertOnSketch(field, expectedFieldToItemsSketchMap.get(field), savedFieldToItemsSketchMap.get(field)));
        }
    }

    private static ArrowRecordBatchFactory<RecordList> createArrowRecordBatchFactory(
            String localDirectory, Schema schema, Consumer<ArrowRecordBatchFactory.Builder<RecordList>> arrowConfig) {
        ArrowRecordBatchFactory.Builder<RecordList> arrowBuilder = ArrowRecordBatchFactory.builder().schema(schema)
                .maxNoOfRecordsToWriteToArrowFileAtOnce(128)
                .localWorkingDirectory(localDirectory)
                .recordWriter(new ArrowRecordWriterAcceptingRecordList());
        arrowConfig.accept(arrowBuilder);
        return arrowBuilder.build();
    }

    private List<RecordList> buildScrambledRecordLists(RecordGenerator.RecordListAndSchema recordListAndSchema) {
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
