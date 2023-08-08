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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.localstack.LocalStackContainer;

import sleeper.core.iterator.IteratorException;
import sleeper.core.iterator.impl.AdditionIterator;
import sleeper.core.key.Key;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.record.RecordComparator;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.ingest.impl.partitionfilewriter.AsyncS3PartitionFileWriterFactory;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.arraylist.ArrayListRecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.ingest.testutils.AwsExternalResource;
import sleeper.ingest.testutils.IngestCoordinatorFactory;
import sleeper.ingest.testutils.IngestCoordinatorTestParameters;
import sleeper.ingest.testutils.QuinFunction;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.ingest.testutils.ResultVerifier;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.parquetConfiguration;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.standardIngestCoordinatorBuilder;
import static sleeper.ingest.testutils.ResultVerifier.createFieldToItemSketchMap;
import static sleeper.ingest.testutils.ResultVerifier.readFieldToItemSketchMap;
import static sleeper.ingest.testutils.ResultVerifier.readMergedRecordsFromPartitionDataFiles;

public class IngestCoordinatorCommonIT {
    @RegisterExtension
    public static final AwsExternalResource AWS_EXTERNAL_RESOURCE = new AwsExternalResource(
            LocalStackContainer.Service.S3,
            LocalStackContainer.Service.DYNAMODB);
    private static final String DATA_BUCKET_NAME = "databucket";
    @TempDir
    public Path temporaryFolder;
    private static final double QUANTILE_SKETCH_TOLERANCE = 0.01;

    private static String newTemporaryDirectory(Path temporaryFolder) {
        try {
            return createTempDirectory(temporaryFolder, null).toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Stream<Arguments> parametersForTests() {
        return Stream.of(
                Arguments.of(Named.of("Direct write, backed by Arrow, no S3",
                        (QuinFunction<StateStore, Schema, String, String, Path, IngestCoordinator<Record>>)
                                (stateStore, sleeperSchema, sleeperIteratorClassName, workingDir, temporaryFolder) ->
                                        (IngestCoordinator<Record>) createIngestCoordinatorDirectWriteBackedByArrow(
                                                stateStore,
                                                sleeperSchema,
                                                newTemporaryDirectory(temporaryFolder),
                                                sleeperIteratorClassName,
                                                workingDir
                                        ))),
                Arguments.of(Named.of("Direct write, backed by Arrow, using S3",
                        (QuinFunction<StateStore, Schema, String, String, Path, IngestCoordinator<Record>>)
                                (stateStore, sleeperSchema, sleeperIteratorClassName, workingDir, temporaryFolder) ->
                                        (IngestCoordinator<Record>) createIngestCoordinatorDirectWriteBackedByArrow(
                                                stateStore,
                                                sleeperSchema,
                                                "s3a://" + DATA_BUCKET_NAME,
                                                sleeperIteratorClassName,
                                                workingDir
                                        ))),
                Arguments.of(Named.of("Async write, backed by Arrow",
                        (QuinFunction<StateStore, Schema, String, String, Path, IngestCoordinator<Record>>)
                                (stateStore, sleeperSchema, sleeperIteratorClassName, workingDir, temporaryFolder) ->
                                        (IngestCoordinator<Record>) createIngestCoordinatorAsyncWriteBackedByArrow(
                                                stateStore,
                                                sleeperSchema,
                                                DATA_BUCKET_NAME,
                                                sleeperIteratorClassName,
                                                workingDir
                                        ))),
                Arguments.of(Named.of("Direct write, backed by ArrayList, no S3",
                        (QuinFunction<StateStore, Schema, String, String, Path, IngestCoordinator<Record>>)
                                (stateStore, sleeperSchema, sleeperIteratorClassName, workingDir, temporaryFolder) ->
                                        (IngestCoordinator<Record>) createIngestCoordinatorDirectWriteBackedByArrayList(
                                                stateStore,
                                                sleeperSchema,
                                                newTemporaryDirectory(temporaryFolder),
                                                sleeperIteratorClassName,
                                                workingDir
                                        ))),
                Arguments.of(Named.of("Direct write, backed by ArrayList, using S3",
                        (QuinFunction<StateStore, Schema, String, String, Path, IngestCoordinator<Record>>)
                                (stateStore, sleeperSchema, sleeperIteratorClassName, workingDir, temporaryFolder) ->
                                        (IngestCoordinator<Record>) createIngestCoordinatorDirectWriteBackedByArrayList(
                                                stateStore,
                                                sleeperSchema,
                                                "s3a://" + DATA_BUCKET_NAME,
                                                sleeperIteratorClassName,
                                                workingDir
                                        )))
        );
    }

    private static Stream<Arguments> parameterObjsForTests() {
        return Stream.of(
                Arguments.of(Named.of("Direct write, backed by Arrow, no S3",
                        IngestCoordinatorFactory.createIngestCoordinatorDirectWriteBackedByArrowWriteToLocalFile())),
                Arguments.of(Named.of("Direct write, backed by Arrow, using S3",
                        IngestCoordinatorFactory.createIngestCoordinatorDirectWriteBackedByArrowWriteToS3())),
                Arguments.of(Named.of("Async write, backed by Arrow",
                        IngestCoordinatorFactory.createIngestCoordinatorAsyncWriteBackedByArrow())),
                Arguments.of(Named.of("Direct write, backed by ArrayList, no S3",
                        IngestCoordinatorFactory.createIngestCoordinatorDirectWriteBackedByArrayListWriteToLocalFile())),
                Arguments.of(Named.of("Direct write, backed by ArrayList, using S3",
                        IngestCoordinatorFactory.createIngestCoordinatorDirectWriteBackedByArrayListWriteToS3()))
        );
    }

    private static IngestCoordinator<Record> createIngestCoordinatorDirectWriteBackedByArrow(
            StateStore stateStore,
            Schema sleeperSchema,
            String filePathPrefix,
            String sleeperIteratorClassName,
            String ingestLocalWorkingDirectory) {
        try {
            ParquetConfiguration parquetConfiguration = parquetConfiguration(
                    sleeperSchema, AWS_EXTERNAL_RESOURCE.getHadoopConfiguration());
            return standardIngestCoordinatorBuilder(
                    stateStore, sleeperSchema,
                    ArrowRecordBatchFactory.builder()
                            .schema(sleeperSchema)
                            .maxNoOfRecordsToWriteToArrowFileAtOnce(128)
                            .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                            .minBatchBufferAllocatorBytes(16 * 1024 * 1024L)
                            .maxBatchBufferAllocatorBytes(16 * 1024 * 1024L)
                            .maxNoOfBytesToWriteLocally(512 * 1024 * 1024L)
                            .localWorkingDirectory(ingestLocalWorkingDirectory)
                            .buildAcceptingRecords(),
                    DirectPartitionFileWriterFactory.from(
                            parquetConfiguration, filePathPrefix))
                    .iteratorClassName(sleeperIteratorClassName)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private static IngestCoordinator<Record> createIngestCoordinatorAsyncWriteBackedByArrow(
            StateStore stateStore,
            Schema sleeperSchema,
            String s3BucketName,
            String sleeperIteratorClassName,
            String ingestLocalWorkingDirectory) {
        try {
            ParquetConfiguration parquetConfiguration = parquetConfiguration(
                    sleeperSchema, AWS_EXTERNAL_RESOURCE.getHadoopConfiguration());
            return standardIngestCoordinatorBuilder(
                    stateStore, sleeperSchema,
                    ArrowRecordBatchFactory.builder()
                            .schema(sleeperSchema)
                            .maxNoOfRecordsToWriteToArrowFileAtOnce(128)
                            .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                            .minBatchBufferAllocatorBytes(16 * 1024 * 1024L)
                            .maxBatchBufferAllocatorBytes(16 * 1024 * 1024L)
                            .maxNoOfBytesToWriteLocally(16 * 1024 * 1024L)
                            .localWorkingDirectory(ingestLocalWorkingDirectory)
                            .buildAcceptingRecords(),
                    AsyncS3PartitionFileWriterFactory.builder()
                            .parquetConfiguration(parquetConfiguration)
                            .s3AsyncClient(AWS_EXTERNAL_RESOURCE.getS3AsyncClient())
                            .localWorkingDirectory(ingestLocalWorkingDirectory)
                            .s3BucketName(s3BucketName)
                            .build())
                    .iteratorClassName(sleeperIteratorClassName)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static IngestCoordinator<Record> createIngestCoordinatorDirectWriteBackedByArrayList(
            StateStore stateStore,
            Schema sleeperSchema,
            String filePathPrefix,
            String sleeperIteratorClassName,
            String ingestLocalWorkingDirectory) {
        try {
            ParquetConfiguration parquetConfiguration = parquetConfiguration(
                    sleeperSchema, AWS_EXTERNAL_RESOURCE.getHadoopConfiguration());
            return standardIngestCoordinatorBuilder(
                    stateStore, sleeperSchema,
                    ArrayListRecordBatchFactory.builder()
                            .parquetConfiguration(parquetConfiguration)
                            .maxNoOfRecordsInLocalStore(1000)
                            .maxNoOfRecordsInMemory(100000)
                            .localWorkingDirectory(ingestLocalWorkingDirectory)
                            .buildAcceptingRecords(),
                    DirectPartitionFileWriterFactory.from(
                            parquetConfiguration, filePathPrefix))
                    .iteratorClassName(sleeperIteratorClassName)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    public void before() {
        AWS_EXTERNAL_RESOURCE.getS3Client().createBucket(DATA_BUCKET_NAME);
    }

    @AfterEach
    public void after() {
        AWS_EXTERNAL_RESOURCE.clear();
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRecordsCorrectly(
            IngestCoordinatorFactory ingestCoordinatorFactoryFn)
            throws StateStoreException, IOException, IteratorException {
        Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(UUID.randomUUID().toString(), recordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getDynamoDBClient()).create();

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();
        stateStore.initialise(tree.getAllPartitions());

        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
        ingestRecords(
                stateStore,
                recordListAndSchema,
                ingestLocalWorkingDirectory,
                null,
                ingestCoordinatorFactoryFn
        );

        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(stateStore.getActiveFiles()).hasSize(1);

        Map<String, List<FileInfo>> partitionIdToFileInfosMap = stateStore.getActiveFiles().stream()
                .collect(Collectors.groupingBy(FileInfo::getPartitionId));

        stateStore.getAllPartitions().forEach(partition -> {
            List<FileInfo> partitionFileInfoList = partitionIdToFileInfosMap.getOrDefault(partition.getId(), Collections.emptyList());

            List<Record> expectedSortedRecordList = Map.of("root", recordListAndSchema.recordList).getOrDefault(partition.getId(), Collections.emptyList()).stream()
                    .sorted(new RecordComparator(recordListAndSchema.sleeperSchema))
                    .collect(Collectors.toList());

            assertThat(expectedSortedRecordList).containsExactlyElementsOf(readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, partitionFileInfoList, hadoopConfiguration));

            Field field = recordListAndSchema.sleeperSchema.getRowKeyFields().get(0);
            ItemsSketch expectedSketch = createFieldToItemSketchMap(recordListAndSchema.sleeperSchema, recordListAndSchema.recordList).get(field);
            ItemsSketch savedSketch = readFieldToItemSketchMap(recordListAndSchema.sleeperSchema, partitionFileInfoList, hadoopConfiguration).get(field);
            ResultVerifier.assertOnSketch(
                    field,
                    savedSketch,
                    expectedSketch
            );
        });
    }

    @ParameterizedTest
    @MethodSource("parametersForTests")
    public void shouldWriteManyRecordsCorrectly(
            QuinFunction<StateStore, Schema, String, String, Path, IngestCoordinator<Record>> ingestCoordinatorFactoryFn)
            throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> 0;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();
        ingestAndVerify(recordListAndSchema,
                tree,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null,
                ingestCoordinatorFactoryFn);
    }

    @ParameterizedTest
    @MethodSource("parametersForTests")
    public void shouldWriteRecordsSplitByPartitionIntKey(
            QuinFunction<StateStore, Schema, String, String, Path, IngestCoordinator<Record>> ingestCoordinatorFactoryFn)
            throws StateStoreException, IOException, IteratorException {
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

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 2)
                .buildTree();

        ingestAndVerify(recordListAndSchema,
                tree,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null,
                ingestCoordinatorFactoryFn);
    }

    @ParameterizedTest
    @MethodSource("parametersForTests")
    public void shouldWriteRecordsSplitByPartitionLongKey(
            QuinFunction<StateStore, Schema, String, String, Path, IngestCoordinator<Record>> ingestCoordinatorFactoryFn)
            throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 2L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 2L)
                .buildTree();

        ingestAndVerify(recordListAndSchema,
                tree,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null,
                ingestCoordinatorFactoryFn);
    }

    @ParameterizedTest
    @MethodSource("parametersForTests")
    public void shouldWriteRecordsSplitByPartitionStringKey(
            QuinFunction<StateStore, Schema, String, String, Path, IngestCoordinator<Record>> ingestCoordinatorFactoryFn)
            throws StateStoreException, IOException, IteratorException {
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
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((String) key.get(0)).compareTo(splitPoint) < 0) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", splitPoint)
                .buildTree();

        ingestAndVerify(recordListAndSchema,
                tree,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null,
                ingestCoordinatorFactoryFn);
    }

    @ParameterizedTest
    @MethodSource("parametersForTests")
    public void shouldWriteRecordsSplitByPartitionByteArrayKey(
            QuinFunction<StateStore, Schema, String, String, Path, IngestCoordinator<Record>> ingestCoordinatorFactoryFn)
            throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new ByteArrayType(),
                Arrays.asList(
                        new byte[]{1, 1},
                        new byte[]{2, 2},
                        new byte[]{64, 65}));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> {
            byte[] byteArray = (byte[]) key.get(0);
            return (byteArray[0] < 64 && byteArray[1] < 64) ? 0 : 1;
        };
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", new byte[]{64, 64})
                .buildTree();

        ingestAndVerify(recordListAndSchema,
                tree,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null,
                ingestCoordinatorFactoryFn);
    }


    @ParameterizedTest
    @MethodSource("parametersForTests")
    public void shouldWriteRecordsSplitByPartitionStringKeyLongSortKey(
            QuinFunction<StateStore, Schema, String, String, Path, IngestCoordinator<Record>> ingestCoordinatorFactoryFn)
            throws StateStoreException, IOException, IteratorException {
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

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "left", "right", 0, splitPoint)
                .buildTree();

        ingestAndVerify(recordListAndSchema,
                tree,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null,
                ingestCoordinatorFactoryFn);
    }

    @ParameterizedTest
    @MethodSource("parametersForTests")
    public void shouldWriteRecordsSplitByPartition2DimensionalByteArrayKey(
            QuinFunction<StateStore, Schema, String, String, Path, IngestCoordinator<Record>> ingestCoordinatorFactoryFn)
            throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey2D(
                new ByteArrayType(), new ByteArrayType(),
                Arrays.asList(new byte[]{1, 1}, new byte[]{11, 2}, new byte[]{64, 65}, new byte[]{5}),
                Arrays.asList(new byte[]{2, 3}, new byte[]{2, 2}, new byte[]{67, 68}, new byte[]{99}));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> {
            byte[] byteArray = (byte[]) key.get(0);
            return (byteArray[0] < 10) ? 0 : 1;
        };
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "left", "right", 0, new byte[]{10})
                .buildTree();

        ingestAndVerify(recordListAndSchema,
                tree,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null,
                ingestCoordinatorFactoryFn);
    }

    @ParameterizedTest
    @MethodSource("parametersForTests")
    public void shouldWriteRecordsSplitByPartition2DimensionalIntLongKeyWhenSplitOnDim1(
            QuinFunction<StateStore, Schema, String, String, Path, IngestCoordinator<Record>> ingestCoordinatorFactoryFn)
            throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey2D(
                new IntType(), new LongType(),
                Arrays.asList(0, 0, 100, 100),
                Arrays.asList(1L, 20L, 1L, 50L));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (key.size() > 1 && ((Long) key.get(1)) < 10L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "left", "right", 1, 10L)
                .buildTree();

        ingestAndVerify(recordListAndSchema,
                tree,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null,
                ingestCoordinatorFactoryFn);
    }

    @ParameterizedTest
    @MethodSource("parametersForTests")
    public void shouldWriteRecordsSplitByPartition2DimensionalLongStringKeyWhenSplitOnDim1(
            QuinFunction<StateStore, Schema, String, String, Path, IngestCoordinator<Record>> ingestCoordinatorFactoryFn)
            throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey2D(
                new LongType(), new StringType(),
                LongStream.range(-100L, 100).boxed().collect(Collectors.toList()),
                LongStream.range(-100L, 100).mapToObj(Long::toString).collect(Collectors.toList()));
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (key.size() > 1 && ((String) key.get(1)).compareTo("2") < 0) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "left", "right", 1, "2")
                .buildTree();

        ingestAndVerify(recordListAndSchema,
                tree,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null,
                ingestCoordinatorFactoryFn);
    }

    @ParameterizedTest
    @MethodSource("parametersForTests")
    public void shouldWriteRecordsSplitByPartitionWhenThereIsOnlyDataInOnePartition(
            QuinFunction<StateStore, Schema, String, String, Path, IngestCoordinator<Record>> ingestCoordinatorFactoryFn)
            throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                Arrays.asList(1L, 0L));

        Function<Key, Integer> keyToPartitionNoMappingFn = key -> (((Long) key.get(0)) < 2L) ? 0 : 1;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1),
                        new AbstractMap.SimpleEntry<>(1, 0))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 2L)
                .buildTree();

        ingestAndVerify(recordListAndSchema,
                tree,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null,
                ingestCoordinatorFactoryFn);
    }

    @ParameterizedTest
    @MethodSource("parametersForTests")
    public void shouldWriteDuplicateRecords(
            QuinFunction<StateStore, Schema, String, String, Path, IngestCoordinator<Record>> ingestCoordinatorFactoryFn)
            throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        RecordGenerator.RecordListAndSchema duplicatedRecordListAndSchema = new RecordGenerator.RecordListAndSchema(
                Stream.of(recordListAndSchema.recordList, recordListAndSchema.recordList)
                        .flatMap(List::stream)
                        .collect(Collectors.toList()),
                recordListAndSchema.sleeperSchema);
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> 0;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();

        ingestAndVerify(duplicatedRecordListAndSchema,
                tree,
                duplicatedRecordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null,
                ingestCoordinatorFactoryFn);
    }

    @ParameterizedTest
    @MethodSource("parametersForTests")
    public void shouldWriteNoRecordsSuccessfully(
            QuinFunction<StateStore, Schema, String, String, Path, IngestCoordinator<Record>> ingestCoordinatorFactoryFn)
            throws StateStoreException, IOException, IteratorException {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                Collections.emptyList());
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> 0;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 0))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();

        ingestAndVerify(recordListAndSchema,
                tree,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                null,
                ingestCoordinatorFactoryFn);
    }

    @ParameterizedTest
    @MethodSource("parametersForTests")
    public void shouldApplyIterator(
            QuinFunction<StateStore, Schema, String, String, Path, IngestCoordinator<Record>> ingestCoordinatorFactoryFn)
            throws StateStoreException, IOException, IteratorException {
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
        Function<Key, Integer> keyToPartitionNoMappingFn = key -> 0;
        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = Stream.of(
                        new AbstractMap.SimpleEntry<>(0, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();
        ingestAndVerify(recordListAndSchema,
                tree,
                expectedAggregatedRecords,
                keyToPartitionNoMappingFn,
                partitionNoToExpectedNoOfFilesMap,
                AdditionIterator.class.getName(),
                ingestCoordinatorFactoryFn);
    }

    private void ingestAndVerify(
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            PartitionTree tree,
            List<Record> expectedRecordsList,
            Function<Key, Integer> keyToPartitionNoMappingFn,
            Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap,
            String sleeperIteratorClassName,
            QuinFunction<StateStore, Schema, String, String, Path, IngestCoordinator<Record>> ingestCoordinatorFactoryFn) throws IOException, StateStoreException, IteratorException {

        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(UUID.randomUUID().toString(), recordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getDynamoDBClient()).create();
        stateStore.initialise(tree.getAllPartitions());

        // A deep working directory forces the ingest coordinator to create a deep tree of directories
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
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

    private void ingestRecords(
            StateStore stateStore,
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            String ingestLocalWorkingDirectory,
            String sleeperIteratorClassName,
            IngestCoordinatorFactory ingestCoordinatorFactoryFn
    ) throws StateStoreException, IteratorException, IOException {
        try (IngestCoordinator<Record> ingestCoordinator =
                     ingestCoordinatorFactoryFn.createIngestCoordinator(
                             IngestCoordinatorTestParameters.builder()
                                     .stateStore(stateStore)
                                     .schema(recordListAndSchema.sleeperSchema)
                                     .iteratorClassName(sleeperIteratorClassName)
                                     .workingDir(ingestLocalWorkingDirectory)
                                     .temporaryFolder(temporaryFolder)
                                     .awsResource(AWS_EXTERNAL_RESOURCE)
                                     .dataBucketName(DATA_BUCKET_NAME)
                                     .build())) {
            for (Record record : recordListAndSchema.recordList) {
                ingestCoordinator.write(record);
            }
        }
    }
}
