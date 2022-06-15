/*
 * Copyright 2022 Crown Copyright
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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.containers.localstack.LocalStackContainer;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.iterator.IteratorException;
import sleeper.core.iterator.impl.AdditionIterator;
import sleeper.core.key.Key;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.ingest.testutils.*;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@RunWith(Parameterized.class)
public class IngestCoordinatorCommonIT {
    @ClassRule
    public static final AwsExternalResource AWS_EXTERNAL_RESOURCE = new AwsExternalResource(
            LocalStackContainer.Service.S3,
            LocalStackContainer.Service.DYNAMODB);
    private static final String DATA_BUCKET_NAME = "databucket";
    private final QuinFunction<StateStore, Schema, String, String, TemporaryFolder, IngestCoordinator<Record>> ingestCoordinatorFactoryFn;
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    public IngestCoordinatorCommonIT(QuinFunction<StateStore, Schema, String, String, TemporaryFolder, IngestCoordinator<Record>> ingestCoordinatorFactoryFn) {
        this.ingestCoordinatorFactoryFn = ingestCoordinatorFactoryFn;
    }

    private static String newTemporaryDirectory(TemporaryFolder temporaryFolder) {
        try {
            return temporaryFolder.newFolder().getAbsolutePath();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parametersForTests() {
        QuinFunction<StateStore, Schema, String, String, TemporaryFolder, IngestCoordinator<Record>> directArrowLocalFn =
                (stateStore, sleeperSchema, sleeperIteratorClassName, workingDir, temporaryFolder) ->
                        (IngestCoordinator<Record>) createIngestCoordinatorDirectWriteBackedByArrow(
                                stateStore,
                                sleeperSchema,
                                newTemporaryDirectory(temporaryFolder),
                                sleeperIteratorClassName,
                                workingDir,
                                temporaryFolder);
        QuinFunction<StateStore, Schema, String, String, TemporaryFolder, IngestCoordinator<Record>> directArrowS3Fn =
                (stateStore, sleeperSchema, sleeperIteratorClassName, workingDir, temporaryFolder) ->
                        (IngestCoordinator<Record>) createIngestCoordinatorDirectWriteBackedByArrow(
                                stateStore,
                                sleeperSchema,
                                "s3a://" + DATA_BUCKET_NAME,
                                sleeperIteratorClassName,
                                workingDir,
                                temporaryFolder);
        QuinFunction<StateStore, Schema, String, String, TemporaryFolder, IngestCoordinator<Record>> asyncArrowS3Fn =
                (stateStore, sleeperSchema, sleeperIteratorClassName, workingDir, temporaryFolder) ->
                        (IngestCoordinator<Record>) createIngestCoordinatorAsyncWriteBackedByArrow(
                                stateStore,
                                sleeperSchema,
                                DATA_BUCKET_NAME,
                                sleeperIteratorClassName,
                                workingDir,
                                temporaryFolder);
        QuinFunction<StateStore, Schema, String, String, TemporaryFolder, IngestCoordinator<Record>> directArrayListLocalFn =
                (stateStore, sleeperSchema, sleeperIteratorClassName, workingDir, temporaryFolder) ->
                        (IngestCoordinator<Record>) createIngestCoordinatorDirectWriteBackedByArrayList(
                                stateStore,
                                sleeperSchema,
                                newTemporaryDirectory(temporaryFolder),
                                sleeperIteratorClassName,
                                workingDir,
                                temporaryFolder);
        QuinFunction<StateStore, Schema, String, String, TemporaryFolder, IngestCoordinator<Record>> directArrayListS3Fn =
                (stateStore, sleeperSchema, sleeperIteratorClassName, workingDir, temporaryFolder) ->
                        (IngestCoordinator<Record>) createIngestCoordinatorDirectWriteBackedByArrayList(
                                stateStore,
                                sleeperSchema,
                                "s3a://" + DATA_BUCKET_NAME,
                                sleeperIteratorClassName,
                                workingDir,
                                temporaryFolder);
        return Arrays.asList(new Object[][]{
                {asyncArrowS3Fn},
                {directArrowLocalFn},
                {directArrowS3Fn},
                {directArrayListLocalFn},
                {directArrayListS3Fn}
        });
    }

    private static IngestCoordinator<Record> createIngestCoordinatorDirectWriteBackedByArrow(
            StateStore stateStore,
            Schema sleeperSchema,
            String filePathPrefix,
            String sleeperIteratorClassName,
            String ingestLocalWorkingDirectory,
            TemporaryFolder temporaryFolder) {
        try {
            String objectFactoryLocalWorkingDirectory = temporaryFolder.newFolder().getAbsolutePath();
            BufferAllocator bufferAllocator = new RootAllocator();
            return StandardIngestCoordinator.directWriteBackedByArrow(
                    new ObjectFactory(new InstanceProperties(), null, objectFactoryLocalWorkingDirectory),
                    stateStore,
                    sleeperSchema,
                    ingestLocalWorkingDirectory,
                    ParquetWriter.DEFAULT_BLOCK_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    "zstd",
                    AWS_EXTERNAL_RESOURCE.getHadoopConfiguration(),
                    sleeperIteratorClassName,
                    null,
                    Integer.MAX_VALUE,
                    filePathPrefix,
                    bufferAllocator,
                    128,
                    1024 * 1024L,
                    1024 * 1024L,
                    1024 * 1024L,
                    512 * 1024 * 1024L);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private static IngestCoordinator<Record> createIngestCoordinatorAsyncWriteBackedByArrow(
            StateStore stateStore,
            Schema sleeperSchema,
            String s3BucketName,
            String sleeperIteratorClassName,
            String ingestLocalWorkingDirectory,
            TemporaryFolder temporaryFolder) {
        try {
            String objectFactoryLocalWorkingDirectory = temporaryFolder.newFolder().getAbsolutePath();
            BufferAllocator bufferAllocator = new RootAllocator();
            return StandardIngestCoordinator.asyncS3WriteBackedByArrow(
                    new ObjectFactory(new InstanceProperties(), null, objectFactoryLocalWorkingDirectory),
                    stateStore,
                    sleeperSchema,
                    ingestLocalWorkingDirectory,
                    ParquetWriter.DEFAULT_BLOCK_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    "zstd",
                    AWS_EXTERNAL_RESOURCE.getHadoopConfiguration(),
                    sleeperIteratorClassName,
                    null,
                    Integer.MAX_VALUE,
                    s3BucketName,
                    AWS_EXTERNAL_RESOURCE.getS3AsyncClient(),
                    bufferAllocator,
                    10,
                    1024 * 1024L,
                    1024 * 1024L,
                    1024 * 1024L,
                    16 * 1024 * 1024L);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static IngestCoordinator<Record> createIngestCoordinatorDirectWriteBackedByArrayList(
            StateStore stateStore,
            Schema sleeperSchema,
            String filePathPrefix,
            String sleeperIteratorClassName,
            String ingestLocalWorkingDirectory,
            TemporaryFolder temporaryFolder) {
        try {
            String objectFactoryLocalWorkingDirectory = temporaryFolder.newFolder().getAbsolutePath();
            return StandardIngestCoordinator.directWriteBackedByArrayList(
                    new ObjectFactory(new InstanceProperties(), null, objectFactoryLocalWorkingDirectory),
                    stateStore,
                    sleeperSchema,
                    ingestLocalWorkingDirectory,
                    ParquetWriter.DEFAULT_BLOCK_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    "zstd",
                    AWS_EXTERNAL_RESOURCE.getHadoopConfiguration(),
                    sleeperIteratorClassName,
                    null,
                    Integer.MAX_VALUE,
                    filePathPrefix,
                    1000,
                    100000L);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void before() {
        AWS_EXTERNAL_RESOURCE.getS3Client().createBucket(DATA_BUCKET_NAME);
    }

    @After
    public void after() {
        AWS_EXTERNAL_RESOURCE.clear();
    }

    @Test
    public void shouldWriteRecordsCorrectly() throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = new ArrayList<>();
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> 0;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerify(recordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null);
    }

    @Test
    public void shouldWriteManyRecordsCorrectly() throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = new ArrayList<>();
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> 0;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerify(recordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null);
    }

    @Test
    public void shouldWriteRecordsSplitByPartitionIntKey() throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new IntType(),
                IntStream.range(-100, 100).boxed().collect(Collectors.toList()));
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = Collections.singletonList(
                Pair.of(Key.create(2), 0));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Integer) key.get(0)) < 2) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerify(recordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null);
    }

    @Test
    public void shouldWriteRecordsSplitByPartitionLongKey() throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = Collections.singletonList(
                Pair.of(Key.create(2L), 0));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 2L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerify(recordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null);
    }

    @Test
    public void shouldWriteRecordsSplitByPartitionStringKey() throws StateStoreException, IOException, IteratorException {
        // RandomStringGenerator generates random unicode strings to test both standard and unusual character sets
        Random random = new Random(0);
        RandomStringGenerator randomStringGenerator = new RandomStringGenerator.Builder()
                .usingRandom(random::nextInt)
                .build();
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new StringType(),
                LongStream.range(-100, 100)
                        .mapToObj(longValue -> String.format("%09d-%s", longValue, randomStringGenerator.generate(random.nextInt(25))))
                        .collect(Collectors.toList()));
        String splitPoint = String.format("%09d", 2);
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = Collections.singletonList(
                Pair.of(Key.create(splitPoint), 0));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((String) key.get(0)).compareTo(splitPoint) < 0) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerify(recordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null);
    }

    @Test
    public void shouldWriteRecordsSplitByPartitionByteArrayKey() throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new ByteArrayType(),
                Arrays.asList(
                        new byte[]{1, 1},
                        new byte[]{2, 2},
                        new byte[]{64, 65}));
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = Collections.singletonList(
                Pair.of(Key.create(new byte[]{64, 64}), 0));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> {
            byte[] byteArray = (byte[]) key.get(0);
            return (byteArray[0] < 64 && byteArray[1] < 64) ? 0 : 1;
        };
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerify(recordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null);
    }


    @Test
    public void shouldWriteRecordsSplitByPartitionStringKeyLongSortKey() throws StateStoreException, IOException, IteratorException {
        // RandomStringGenerator generates random unicode strings to test both standard and unusual character sets
        Random random = new Random(0);
        RandomStringGenerator randomStringGenerator = new RandomStringGenerator.Builder()
                .usingRandom(random::nextInt)
                .build();
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1DSort1D(
                new StringType(),
                new LongType(),
                LongStream.range(-100, 100).boxed()
                        .map(longValue -> String.format("%09d-%s", longValue, randomStringGenerator.generate(random.nextInt(25))))
                        .flatMap(str -> Stream.of(str, str, str))
                        .collect(Collectors.toList()),
                LongStream.range(-100, 100).boxed()
                        .flatMap(longValue -> Stream.of(longValue + 1000L, longValue, longValue - 1000L))
                        .collect(Collectors.toList()));
        String splitPoint = String.format("%09d", 2);
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = Collections.singletonList(
                Pair.of(Key.create(splitPoint), 0));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((String) key.get(0)).compareTo(splitPoint) < 0) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerify(recordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null);
    }

    @Test
    public void shouldWriteRecordsSplitByPartition2DimensionalByteArrayKey() throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey2D(
                new ByteArrayType(), new ByteArrayType(),
                Arrays.asList(new byte[]{1, 1}, new byte[]{11, 2}, new byte[]{64, 65}, new byte[]{5}),
                Arrays.asList(new byte[]{2, 3}, new byte[]{2, 2}, new byte[]{67, 68}, new byte[]{99}));
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = Collections.singletonList(
                Pair.of(Key.create(Arrays.asList(new byte[]{10}, null)), 0));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> {
            byte[] byteArray = (byte[]) key.get(0);
            return (byteArray[0] < 10) ? 0 : 1;
        };
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerify(recordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null);
    }

    @Test
    public void shouldWriteRecordsSplitByPartition2DimensionalIntLongKeyWhenSplitOnDim1() throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey2D(
                new IntType(), new LongType(),
                Arrays.asList(0, 0, 100, 100),
                Arrays.asList(1L, 20L, 1L, 50L));
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = Collections.singletonList(
                Pair.of(Key.create(Arrays.asList(0, 10L)), 1));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (key.size() > 1 && ((Long) key.get(1)) < 10L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerify(recordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null);
    }

    @Test
    public void shouldWriteRecordsSplitByPartition2DimensionalLongStringKeyWhenSplitOnDim1() throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey2D(
                new LongType(), new StringType(),
                LongStream.range(-100L, 100).boxed().collect(Collectors.toList()),
                LongStream.range(-100L, 100).mapToObj(Long::toString).collect(Collectors.toList()));
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = Collections.singletonList(
                Pair.of(Key.create(Arrays.asList(0L, "2")), 1));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (key.size() > 1 && ((String) key.get(1)).compareTo("2") < 0) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerify(recordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null);
    }

    @Test
    public void shouldWriteRecordsSplitByPartitionWhenThereIsOnlyDataInOnePartition() throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                Arrays.asList(1L, 0L));
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = Collections.singletonList(
                Pair.of(Key.create(2L), 0));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 2L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 0))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerify(recordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null);
    }

    @Test
    public void shouldWriteDuplicateRecords() throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        RecordGenerator.RecordListAndSchema duplicatedRecordListAndSchema = new RecordGenerator.RecordListAndSchema(
                Stream.of(recordListAndSchema.recordList, recordListAndSchema.recordList)
                        .flatMap(List::stream)
                        .collect(Collectors.toList()),
                recordListAndSchema.sleeperSchema);
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = new ArrayList<>();
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> 0;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerify(duplicatedRecordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                duplicatedRecordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null);
    }

    @Test
    public void shouldWriteNoRecordsSuccessfully() throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                Collections.emptyList());
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = new ArrayList<>();
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> 0;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 0))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerify(recordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null);
    }

    @Test
    public void shouldApplyIterator() throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.byteArrayRowKeyLongSortKey(
                Arrays.asList(new byte[]{1, 1}, new byte[]{1, 1}, new byte[]{11, 12}, new byte[]{11, 12}),
                Arrays.asList(1L, 1L, 2L, 2L),
                Arrays.asList(1L, 2L, 3L, 4L));
        Record expectedRecord1 = new Record();
        expectedRecord1.put(recordListAndSchema.sleeperSchema.getRowKeyFieldNames().get(0), new byte[]{1, 1});
        expectedRecord1.put(recordListAndSchema.sleeperSchema.getSortKeyFieldNames().get(0), 1L);
        expectedRecord1.put(recordListAndSchema.sleeperSchema.getValueFieldNames().get(0), 3L);
        Record expectedRecord2 = new Record();
        expectedRecord2.put(recordListAndSchema.sleeperSchema.getRowKeyFieldNames().get(0), new byte[]{11, 12});
        expectedRecord2.put(recordListAndSchema.sleeperSchema.getSortKeyFieldNames().get(0), 2L);
        expectedRecord2.put(recordListAndSchema.sleeperSchema.getValueFieldNames().get(0), 7L);
        List<Record> expectedAggregatedRecords = Arrays.asList(expectedRecord1, expectedRecord2);
        List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder = Collections.emptyList();
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> 0;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ingestAndVerify(recordListAndSchema,
                keyAndDimensionToSplitOnInOrder,
                expectedAggregatedRecords,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                AdditionIterator.class.getName());
    }

    private void ingestAndVerify(
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder,
            List<Record> expectedRecordsList,
            Function<Key, Integer> keyToPartitionNoMappingFn,
            Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap,
            String sleeperIteratorClassName) throws IOException, StateStoreException, IteratorException {
        StateStore stateStore = PartitionedTableCreator.createStateStore(
                AWS_EXTERNAL_RESOURCE.getDynamoDBClient(),
                recordListAndSchema.sleeperSchema,
                keyAndDimensionToSplitOnInOrder);
        // A deep working directory forces the ingest coordinator to create a deep tree of directories
        String ingestLocalWorkingDirectory = temporaryFolder.newFolder().getAbsolutePath() + "/path/to/new/sub/directory";
        try (IngestCoordinator<Record> ingestCoordinator =
                     ingestCoordinatorFactoryFn.apply(
                             stateStore,
                             recordListAndSchema.sleeperSchema,
                             sleeperIteratorClassName,
                             ingestLocalWorkingDirectory,
                             temporaryFolder)) {
            for (Record record : recordListAndSchema.recordList) {
                ingestCoordinator.write(record);
            }
        }
        ResultVerifier.verify(
                stateStore,
                recordListAndSchema.sleeperSchema,
                keyToPartitionNoMappingFn,
                expectedRecordsList,
                partitionNoToExpectedNoOfFilesMap,
                AWS_EXTERNAL_RESOURCE.getHadoopConfiguration(),
                ingestLocalWorkingDirectory);
    }
}
