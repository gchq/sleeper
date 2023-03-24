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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;

import sleeper.core.iterator.IteratorException;
import sleeper.core.key.Key;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriter;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriterAcceptingRecords;
import sleeper.ingest.testutils.AwsExternalResource;
import sleeper.ingest.testutils.PartitionedTableCreator;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.ingest.testutils.ResultVerifier;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
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

public class IngestCoordinatorBespokeUsingDirectWriteBackedByArrowRecordWriterAcceptingRecordListIT {
    @RegisterExtension
    public static final AwsExternalResource AWS_EXTERNAL_RESOURCE = new AwsExternalResource(
            LocalStackContainer.Service.S3,
            LocalStackContainer.Service.DYNAMODB);
    private static final String DATA_BUCKET_NAME = "databucket";
    @TempDir
    public Path temporaryFolder;

    @BeforeEach
    public void before() {
        AWS_EXTERNAL_RESOURCE.getS3Client().createBucket(DATA_BUCKET_NAME);
    }

    @AfterEach
    public void after() {
        AWS_EXTERNAL_RESOURCE.clear();
    }

    @Test
    public void shouldWriteRecordsWhenThereAreMoreRecordsInAPartitionThanCanFitInMemory() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = Collections.singletonList(
                Pair.of(Key.create(0L), 0));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 0L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerifyUsingDirectWriteBackedByArrow(
                recordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                16 * 1024 * 1024L,
                4 * 1024 * 1024L,
                128 * 1024 * 1024L);
    }

    @Test
    public void shouldWriteRecordsWhenThereAreMoreRecordsThanCanFitInLocalFile() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = Collections.singletonList(
                Pair.of(Key.create(0L), 0));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 0L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 2),
                        new AbstractMap.SimpleEntry<>(1, 2))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerifyUsingDirectWriteBackedByArrow(
                recordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                16 * 1024 * 1024L,
                4 * 1024 * 1024L,
                16 * 1024 * 1024L);
    }

    @Test
    public void shouldErrorWhenBatchBufferAndWorkingBufferAreSmall() {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = Collections.singletonList(
                Pair.of(Key.create(0L), 0));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 0L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 2),
                        new AbstractMap.SimpleEntry<>(1, 2))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertThatThrownBy(() ->
                ingestAndVerifyUsingDirectWriteBackedByArrow(
                        recordListAndSchema,
                        keyAndDimensionToSplitOnInOrder,
                        keyToPartitionNoMappingFn,
                        partitionNoToExpectedNoOfFilesMap,
                        32 * 1024L,
                        1024 * 1024L,
                        64 * 1024 * 1024L))
                .isInstanceOf(OutOfMemoryException.class)
                .hasNoSuppressedExceptions();
    }

    private void ingestAndVerifyUsingDirectWriteBackedByArrow(
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder,
            Function<Key, Integer> keyToPartitionNoMappingFn,
            Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap,
            long arrowWorkingBytes,
            long arrowBatchBytes,
            long localStoreBytes) throws IOException, StateStoreException, IteratorException {
        StateStore stateStore = PartitionedTableCreator.createStateStore(
                AWS_EXTERNAL_RESOURCE.getDynamoDBClient(),
                recordListAndSchema.sleeperSchema,
                keyAndDimensionToSplitOnInOrder);
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString();

        ParquetConfiguration parquetConfiguration = parquetConfiguration(
                recordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getHadoopConfiguration());
        try (IngestCoordinator<RecordList> ingestCoordinator = standardIngestCoordinator(
                stateStore, recordListAndSchema.sleeperSchema,
                ArrowRecordBatchFactory.builder()
                        .schema(recordListAndSchema.sleeperSchema)
                        .maxNoOfRecordsToWriteToArrowFileAtOnce(128)
                        .workingBufferAllocatorBytes(arrowWorkingBytes)
                        .minBatchBufferAllocatorBytes(arrowBatchBytes)
                        .maxBatchBufferAllocatorBytes(arrowBatchBytes)
                        .maxNoOfBytesToWriteLocally(localStoreBytes)
                        .localWorkingDirectory(ingestLocalWorkingDirectory)
                        .recordWriter(new ArrowRecordWriterAcceptingRecordList())
                        .build(),
                DirectPartitionFileWriterFactory.from(
                        parquetConfiguration, "s3a://" + DATA_BUCKET_NAME))) {

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
            for (RecordList recordList : recordLists) {
                ingestCoordinator.write(recordList);
            }
        }

        ResultVerifier.verify(
                stateStore,
                recordListAndSchema.sleeperSchema,
                keyToPartitionNoMappingFn,
                recordListAndSchema.recordList,
                partitionNoToExpectedNoOfFilesMap,
                AWS_EXTERNAL_RESOURCE.getHadoopConfiguration(),
                ingestLocalWorkingDirectory);
    }

    public static class RecordList {
        private final List<Record> records;

        public RecordList() {
            this.records = new ArrayList<>();
        }

        public void addRecord(Record record) {
            this.records.add(record);
        }

        public List<Record> getRecords() {
            return records;
        }
    }

    public static class ArrowRecordWriterAcceptingRecordList implements ArrowRecordWriter<RecordList> {

        @Override
        public void insert(List<Field> allFields, VectorSchemaRoot vectorSchemaRoot, RecordList recordList, int insertAtRowNo) {
            for (Record record : recordList.getRecords()) {
                for (int fieldNo = 0; fieldNo < allFields.size(); fieldNo++) {
                        Field sleeperField = allFields.get(fieldNo);
                        String fieldName = sleeperField.getName();
                        Type sleeperType = sleeperField.getType();
                        if (sleeperType instanceof IntType) {
                            IntVector intVector = (IntVector) vectorSchemaRoot.getVector(fieldNo);
                            Integer value = (Integer) record.get(fieldName);
                            intVector.setSafe(insertAtRowNo, value);
                        } else if (sleeperType instanceof LongType) {
                            BigIntVector bigIntVector = (BigIntVector) vectorSchemaRoot.getVector(fieldNo);
                            Long value = (Long) record.get(fieldName);
                            bigIntVector.setSafe(insertAtRowNo, value);
                        } else if (sleeperType instanceof StringType) {
                            VarCharVector varCharVector = (VarCharVector) vectorSchemaRoot.getVector(fieldNo);
                            String value = (String) record.get(fieldName);
                            varCharVector.setSafe(insertAtRowNo, value.getBytes(StandardCharsets.UTF_8));
                        } else if (sleeperType instanceof ByteArrayType) {
                            VarBinaryVector varBinaryVector = (VarBinaryVector) vectorSchemaRoot.getVector(fieldNo);
                            byte[] value = (byte[]) record.get(fieldName);
                            varBinaryVector.setSafe(insertAtRowNo, value);
                        } else if (sleeperType instanceof ListType) {
                            ArrowRecordWriterAcceptingRecords.writeList(
                                ((ListType) sleeperType).getElementType(),
                                (List<?>) record.get(fieldName),
                                (ListVector) vectorSchemaRoot.getVector(fieldNo),
                                insertAtRowNo);
                        } else if (sleeperType instanceof MapType) {
                            ArrowRecordWriterAcceptingRecords.writeMap(
                                ((MapType) sleeperType).getKeyType(),
                                ((MapType) sleeperType).getValueType(),
                                (Map<?, ?>) record.get(fieldName),
                                (ListVector) vectorSchemaRoot.getVector(fieldNo),
                                insertAtRowNo);
                        } else {
                            throw new UnsupportedOperationException("Sleeper column type " + sleeperType.toString() + " is not handled");
                        }
                }
            }
            vectorSchemaRoot.setRowCount(insertAtRowNo + recordList.getRecords().size());
        }
    }
}
