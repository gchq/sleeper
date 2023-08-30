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

import org.apache.commons.text.RandomStringGenerator;
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
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.testutils.AwsExternalResource;
import sleeper.ingest.testutils.IngestCoordinatorTestParameters;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.ingest.testutils.ResultVerifier;
import sleeper.ingest.testutils.TestIngestType;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.testutils.ResultVerifier.readMergedRecordsFromPartitionDataFiles;

public class IngestCoordinatorCommonIT {
    @RegisterExtension
    public static final AwsExternalResource AWS_EXTERNAL_RESOURCE = new AwsExternalResource(
            LocalStackContainer.Service.S3,
            LocalStackContainer.Service.DYNAMODB);
    private static final String DATA_BUCKET_NAME = "databucket";
    @TempDir
    public Path temporaryFolder;

    private static Stream<Arguments> parameterObjsForTests() {
        return Stream.of(
                Arguments.of(Named.of("Direct write, backed by Arrow, no S3",
                        TestIngestType.directWriteBackedByArrowWriteToLocalFile())),
                Arguments.of(Named.of("Direct write, backed by Arrow, using S3",
                        TestIngestType.directWriteBackedByArrowWriteToS3())),
                Arguments.of(Named.of("Async write, backed by Arrow",
                        TestIngestType.asyncWriteBackedByArrow())),
                Arguments.of(Named.of("Direct write, backed by ArrayList, no S3",
                        TestIngestType.directWriteBackedByArrayListWriteToLocalFile())),
                Arguments.of(Named.of("Direct write, backed by ArrayList, using S3",
                        TestIngestType.directWriteBackedByArrayListWriteToS3()))
        );
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
    public void shouldWriteRecordsCorrectly(TestIngestType ingestType)
            throws StateStoreException, IOException, IteratorException {
        // Given
        Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(UUID.randomUUID().toString(), recordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getDynamoDBClient()).create();

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();
        stateStore.initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("rootFile"))
                .fileUpdatedTimes(List.of(stateStoreUpdateTime))
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(recordListAndSchema, parameters, ingestType);

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileInfo fileInfo = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build()
                .rootFile(ingestType.getFilePrefix(parameters) + "/partition_root/rootFile.parquet", 200, -100L, 99L);

        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrder(fileInfo);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(actualRecords).extracting(record -> record.getValues(List.of("key0")))
                .containsExactlyElementsOf(LongStream.range(-100, 100).boxed()
                        .map(List::<Object>of)
                        .collect(Collectors.toList()));

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteManyRecordsCorrectly(TestIngestType ingestType)
            throws StateStoreException, IOException, IteratorException {
        // Given
        Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(UUID.randomUUID().toString(), recordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getDynamoDBClient()).create();

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();
        stateStore.initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("rootFile"))
                .fileUpdatedTimes(List.of(stateStoreUpdateTime))
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(recordListAndSchema, parameters, ingestType);

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileInfo fileInfo = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build()
                .rootFile(ingestType.getFilePrefix(parameters) + "/partition_root/rootFile.parquet", 20000, -10000L, 9999L);

        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrder(fileInfo);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(actualRecords).extracting(record -> record.getValues(List.of("key0")))
                .containsExactlyElementsOf(LongStream.range(-10000, 10000).boxed()
                        .map(List::<Object>of)
                        .collect(Collectors.toList()));

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRecordsSplitByPartitionIntKey(
            TestIngestType ingestType)
            throws StateStoreException, IOException, IteratorException {
        Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new IntType(),
                IntStream.range(-100, 100).boxed().collect(Collectors.toList()));
        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(UUID.randomUUID().toString(), recordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getDynamoDBClient()).create();

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 2)
                .buildTree();
        stateStore.initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .fileUpdatedTimes(List.of(stateStoreUpdateTime, stateStoreUpdateTime, stateStoreUpdateTime))
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(recordListAndSchema, parameters, ingestType);

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        List<FileInfo> fileInfoList = List.of(
                fileInfoFactory.leafFile(ingestType.getFilePrefix(parameters) + "/partition_right/rightFile.parquet", 98, 2, 99),
                fileInfoFactory.leafFile(ingestType.getFilePrefix(parameters) + "/partition_left/leftFile.parquet", 102, -100, 1)
        );

        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(fileInfoList);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(actualRecords).extracting(record -> record.getValues(List.of("key0")))
                .containsExactlyElementsOf(IntStream.range(-100, 100).boxed()
                        .map(List::<Object>of)
                        .collect(Collectors.toList()));

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRecordsSplitByPartitionLongKey(
            TestIngestType ingestType)
            throws StateStoreException, IOException, IteratorException {
        Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(UUID.randomUUID().toString(), recordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getDynamoDBClient()).create();

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 2L)
                .buildTree();
        stateStore.initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .fileUpdatedTimes(List.of(stateStoreUpdateTime, stateStoreUpdateTime, stateStoreUpdateTime))
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(recordListAndSchema, parameters, ingestType);

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        List<FileInfo> fileInfoList = List.of(
                fileInfoFactory.leafFile(ingestType.getFilePrefix(parameters) + "/partition_right/rightFile.parquet", 98, 2L, 99L),
                fileInfoFactory.leafFile(ingestType.getFilePrefix(parameters) + "/partition_left/leftFile.parquet", 102, -100L, 1L)
        );

        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(fileInfoList);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(actualRecords).extracting(record -> record.getValues(List.of("key0")))
                .containsExactlyElementsOf(LongStream.range(-100, 100).boxed()
                        .map(List::<Object>of)
                        .collect(Collectors.toList()));

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRecordsSplitByPartitionStringKey(
            TestIngestType ingestType)
            throws Exception {
        Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();
        // RandomStringGenerator generates random unicode strings to test both standard and unusual character sets
        Supplier<String> randomString = randomStringGeneratorWithMaxLength(25);
        List<String> keys = LongStream.range(0, 200)
                .mapToObj(longValue -> String.format("%09d-%s", longValue, randomString.get()))
                .collect(Collectors.toList());
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new StringType(), keys);
        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(UUID.randomUUID().toString(), recordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getDynamoDBClient()).create();
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", "000000102")
                .buildTree();

        stateStore.initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .fileUpdatedTimes(List.of(stateStoreUpdateTime, stateStoreUpdateTime, stateStoreUpdateTime))
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(recordListAndSchema, parameters, ingestType);


        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        List<FileInfo> fileInfoList = List.of(
                fileInfoFactory.leafFile(ingestType.getFilePrefix(parameters) + "/partition_right/rightFile.parquet", 98, keys.get(102), keys.get(199)),
                fileInfoFactory.leafFile(ingestType.getFilePrefix(parameters) + "/partition_left/leftFile.parquet", 102, keys.get(0), keys.get(101))
        );

        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(fileInfoList);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(actualRecords.stream().map(record -> record.getValues(List.of("key0")).get(0)).collect(Collectors.toList()))
                .containsExactlyElementsOf(keys);

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRecordsSplitByPartitionByteArrayKey(
            TestIngestType ingestType)
            throws StateStoreException, IOException, IteratorException {
        Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new ByteArrayType(),
                Arrays.asList(
                        new byte[]{1, 1},
                        new byte[]{2, 2},
                        new byte[]{64, 65}));

        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(UUID.randomUUID().toString(), recordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getDynamoDBClient()).create();
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", new byte[]{64, 64})
                .buildTree();
        stateStore.initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .fileUpdatedTimes(List.of(stateStoreUpdateTime, stateStoreUpdateTime, stateStoreUpdateTime))
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(recordListAndSchema, parameters, ingestType);

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        List<FileInfo> fileInfoList = List.of(
                fileInfoFactory.leafFile(ingestType.getFilePrefix(parameters) + "/partition_left/leftFile.parquet", 2, new byte[]{1, 1}, new byte[]{2, 2}),
                fileInfoFactory.leafFile(ingestType.getFilePrefix(parameters) + "/partition_right/rightFile.parquet", 1, new byte[]{64, 65}, new byte[]{64, 65})
        );

        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(fileInfoList);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(actualRecords
                .stream().map(record -> record.getValues(List.of("key0")).get(0)).collect(Collectors.toList()))
                .containsExactly(new byte[]{1, 1}, new byte[]{2, 2}, new byte[]{64, 65});

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRecordsSplitByPartitionStringKeyLongSortKey(
            TestIngestType ingestType)
            throws Exception {

        Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();
        // RandomStringGenerator generates random unicode strings to test both standard and unusual character sets
        Supplier<String> randomString = randomStringGeneratorWithMaxLength(25);
        List<String> stringKeys = LongStream.range(0, 200)
                .mapToObj(longValue -> String.format("%09d-%s", longValue, randomString.get()))
                .flatMap(str -> Stream.of(str, str, str))
                .collect(Collectors.toList());
        List<Long> longKeys = LongStream.range(-100, 100).boxed()
                .flatMap(longValue -> Stream.of(longValue - 1000L, longValue, longValue + 1000L))
                .collect(Collectors.toList());
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1DSort1D(
                new StringType(),
                new LongType(),
                stringKeys, longKeys);

        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(UUID.randomUUID().toString(), recordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getDynamoDBClient()).create();
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "left", "right", 0, "000000102")
                .buildTree();
        stateStore.initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .fileUpdatedTimes(List.of(stateStoreUpdateTime, stateStoreUpdateTime, stateStoreUpdateTime))
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(recordListAndSchema, parameters, ingestType);

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        List<FileInfo> fileInfoList = List.of(
                fileInfoFactory.leafFile(ingestType.getFilePrefix(parameters) + "/partition_right/rightFile.parquet", 294, stringKeys.get(306), stringKeys.get(597)),
                fileInfoFactory.leafFile(ingestType.getFilePrefix(parameters) + "/partition_left/leftFile.parquet", 306, stringKeys.get(0), stringKeys.get(303))
        );

        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(fileInfoList);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(actualRecords.stream().map(record -> record.getValues(List.of("key0")).get(0)).collect(Collectors.toList()))
                .containsExactlyElementsOf(stringKeys);
        assertThat(actualRecords.stream().map(record -> record.getValues(List.of("sortKey0")).get(0)).collect(Collectors.toList()))
                .containsExactlyElementsOf(longKeys);

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRecordsSplitByPartition2DimensionalByteArrayKey(
            TestIngestType ingestType)
            throws StateStoreException, IOException, IteratorException {
        Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey2D(
                new ByteArrayType(), new ByteArrayType(),
                Arrays.asList(new byte[]{1, 1}, new byte[]{11, 2}, new byte[]{64, 65}, new byte[]{5}),
                Arrays.asList(new byte[]{2, 3}, new byte[]{2, 2}, new byte[]{67, 68}, new byte[]{99}));

        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(UUID.randomUUID().toString(), recordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getDynamoDBClient()).create();
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "left", "right", 0, new byte[]{10})
                .buildTree();
        stateStore.initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .fileUpdatedTimes(List.of(stateStoreUpdateTime, stateStoreUpdateTime, stateStoreUpdateTime))
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(recordListAndSchema, parameters, ingestType);

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        List<FileInfo> fileInfoList = List.of(
                fileInfoFactory.partitionFile("right", ingestType.getFilePrefix(parameters) +
                        "/partition_right/rightFile.parquet", 2, new byte[]{11, 2}, new byte[]{64, 65}),
                fileInfoFactory.partitionFile("left", ingestType.getFilePrefix(parameters) +
                        "/partition_left/leftFile.parquet", 2, new byte[]{1, 1}, new byte[]{5})
        );

        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(fileInfoList);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(actualRecords).extracting(record -> record.getValues(List.of("key0")).get(0))
                .containsExactly(new byte[]{1, 1}, new byte[]{5}, new byte[]{11, 2}, new byte[]{64, 65});
        assertThat(actualRecords).extracting(record -> record.getValues(List.of("key1")).get(0))
                .containsExactly(new byte[]{2, 3}, new byte[]{99}, new byte[]{2, 2}, new byte[]{67, 68});
        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(1),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRecordsSplitByPartition2DimensionalIntLongKeyWhenSplitOnDim1(
            TestIngestType ingestType)
            throws StateStoreException, IOException, IteratorException {
        Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey2D(
                new IntType(), new LongType(),
                Arrays.asList(0, 0, 100, 100),
                Arrays.asList(1L, 20L, 1L, 50L));
        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(UUID.randomUUID().toString(), recordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getDynamoDBClient()).create();

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "left", "right", 1, 10L)
                .buildTree();

        stateStore.initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .fileUpdatedTimes(List.of(stateStoreUpdateTime, stateStoreUpdateTime, stateStoreUpdateTime))
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(recordListAndSchema, parameters, ingestType);

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        List<FileInfo> fileInfoList = List.of(
                fileInfoFactory.partitionFile("right", ingestType.getFilePrefix(parameters) +
                        "/partition_right/rightFile.parquet", 2, 0, 100),
                fileInfoFactory.partitionFile("left", ingestType.getFilePrefix(parameters) +
                        "/partition_left/leftFile.parquet", 2, 0, 100)
        );

        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(fileInfoList);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(actualRecords).extracting(record -> record.getValues(List.of("key0")))
                .containsExactly(List.of(0), List.of(0), List.of(100), List.of(100));
        assertThat(actualRecords).extracting(record -> record.getValues(List.of("key1")))
                .containsExactly(List.of(1L), List.of(20L), List.of(1L), List.of(50L));
        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(1),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRecordsSplitByPartition2DimensionalLongStringKeyWhenSplitOnDim1(
            TestIngestType ingestType)
            throws StateStoreException, IOException, IteratorException {
        Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey2D(
                new LongType(), new StringType(),
                LongStream.range(-100L, 100L).boxed().collect(Collectors.toList()),
                LongStream.range(-100L, 100L).mapToObj(Long::toString).collect(Collectors.toList()));
        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(UUID.randomUUID().toString(), recordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getDynamoDBClient()).create();

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "left", "right", 1, "2")
                .buildTree();
        stateStore.initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .fileUpdatedTimes(List.of(stateStoreUpdateTime, stateStoreUpdateTime, stateStoreUpdateTime))
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(recordListAndSchema, parameters, ingestType);

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        List<FileInfo> fileInfoList = List.of(
                fileInfoFactory.partitionFile("left", ingestType.getFilePrefix(parameters) +
                        "/partition_left/leftFile.parquet", 112, -100L, 19L),
                fileInfoFactory.partitionFile("right", ingestType.getFilePrefix(parameters) +
                        "/partition_right/rightFile.parquet", 88, 2L, 99L)
        );

        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(fileInfoList);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(actualRecords).extracting(record -> record.getValues(List.of("key0")))
                .containsExactlyElementsOf(
                        LongStream.range(-100L, 100L)
                                .boxed()
                                .map(longValue -> List.of((Object) longValue))
                                .collect(Collectors.toList())
                );

        assertThat(actualRecords).extracting(record -> record.getValues(List.of("key1")))
                .containsExactlyElementsOf(
                        LongStream.range(-100L, 100L)
                                .boxed()
                                .map(x -> List.of((Object) String.valueOf(x)))
                                .collect(Collectors.toList())
                );

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRecordsSplitByPartitionWhenThereIsOnlyDataInOnePartition(
            TestIngestType ingestType)
            throws StateStoreException, IOException, IteratorException {
        Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                Arrays.asList(1L, 0L));
        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(UUID.randomUUID().toString(), recordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getDynamoDBClient()).create();

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 2L)
                .buildTree();
        stateStore.initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .fileUpdatedTimes(List.of(stateStoreUpdateTime, stateStoreUpdateTime, stateStoreUpdateTime))
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(recordListAndSchema, parameters, ingestType);

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        List<FileInfo> fileInfoList = List.of(
                fileInfoFactory.leafFile(ingestType.getFilePrefix(parameters) + "/partition_left/leftFile.parquet", 2, 0L, 1L)
        );
        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(fileInfoList);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(actualRecords).extracting(record -> record.getValues(List.of("key0")))
                .containsExactly(List.of(0L), List.of(1L));

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteDuplicateRecords(
            TestIngestType ingestType)
            throws StateStoreException, IOException, IteratorException {
        Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        RecordGenerator.RecordListAndSchema duplicatedRecordListAndSchema = new RecordGenerator.RecordListAndSchema(
                Stream.of(recordListAndSchema.recordList, recordListAndSchema.recordList)
                        .flatMap(List::stream)
                        .collect(Collectors.toList()),
                recordListAndSchema.sleeperSchema);
        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(UUID.randomUUID().toString(), duplicatedRecordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getDynamoDBClient()).create();

        PartitionTree tree = new PartitionsBuilder(duplicatedRecordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();
        stateStore.initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .fileUpdatedTimes(List.of(stateStoreUpdateTime, stateStoreUpdateTime, stateStoreUpdateTime))
                .stateStore(stateStore)
                .schema(duplicatedRecordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(duplicatedRecordListAndSchema, parameters, ingestType);

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(duplicatedRecordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(duplicatedRecordListAndSchema.sleeperSchema)
                .build();
        List<FileInfo> fileInfoList = List.of(
                fileInfoFactory.leafFile(ingestType.getFilePrefix(parameters) + "/partition_root/leftFile.parquet", 400, -100L, 99L)
        );
        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(fileInfoList);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(duplicatedRecordListAndSchema.recordList);
        assertThat(actualRecords).extracting(record -> record.getValues(List.of("key0")).get(0))
                .containsExactlyElementsOf(LongStream.range(-100, 100).boxed().flatMap(longValue -> Stream.of(longValue, longValue)).collect(Collectors.toList()));

        ResultVerifier.assertOnSketch(
                duplicatedRecordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                duplicatedRecordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );

    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteNoRecordsSuccessfully(TestIngestType ingestType)
            throws StateStoreException, IOException, IteratorException {
        Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                Collections.emptyList());
        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(UUID.randomUUID().toString(), recordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getDynamoDBClient()).create();

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();
        stateStore.initialise(tree.getAllPartitions());
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of())
                .fileUpdatedTimes(List.of())
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(recordListAndSchema, parameters, ingestType);

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        assertThat(actualFiles).isEmpty();
        assertThat(actualRecords).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldApplyIterator(
            TestIngestType ingestType)
            throws StateStoreException, IOException, IteratorException {
        Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.byteArrayRowKeyLongSortKey(
                Arrays.asList(new byte[]{1, 1}, new byte[]{1, 1}, new byte[]{11, 12}, new byte[]{11, 12}),
                Arrays.asList(1L, 1L, 2L, 2L),
                Arrays.asList(1L, 2L, 3L, 4L));
        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(UUID.randomUUID().toString(), recordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getDynamoDBClient()).create();
        Record expectedRecord1 = new Record();
        expectedRecord1.put(recordListAndSchema.sleeperSchema.getRowKeyFieldNames().get(0), new byte[]{1, 1});
        expectedRecord1.put(recordListAndSchema.sleeperSchema.getSortKeyFieldNames().get(0), 1L);
        expectedRecord1.put(recordListAndSchema.sleeperSchema.getValueFieldNames().get(0), 3L);
        Record expectedRecord2 = new Record();
        expectedRecord2.put(recordListAndSchema.sleeperSchema.getRowKeyFieldNames().get(0), new byte[]{11, 12});
        expectedRecord2.put(recordListAndSchema.sleeperSchema.getSortKeyFieldNames().get(0), 2L);
        expectedRecord2.put(recordListAndSchema.sleeperSchema.getValueFieldNames().get(0), 7L);
        recordListAndSchema.recordList = Arrays.asList(expectedRecord1, expectedRecord2);

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();

        stateStore.initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("rootFile"))
                .fileUpdatedTimes(List.of(stateStoreUpdateTime, stateStoreUpdateTime, stateStoreUpdateTime))
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        ingestRecords(recordListAndSchema, parameters, ingestType);

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        List<FileInfo> fileInfoList = List.of(
                fileInfoFactory.leafFile(ingestType.getFilePrefix(parameters) + "/partition_root/rootFile.parquet", 2, new byte[]{1, 1}, new byte[]{11, 12})
        );
        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(fileInfoList);
        assertThat(actualRecords).containsExactlyElementsOf(recordListAndSchema.recordList);

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
    }

    private Supplier<String> randomStringGeneratorWithMaxLength(Integer maxLength) {
        Random random = new Random(0);
        RandomStringGenerator randomStringGenerator = new RandomStringGenerator.Builder()
                .usingRandom(random::nextInt)
                .build();
        return () -> randomStringGenerator.generate(random.nextInt(maxLength));
    }

    private void ingestRecords(
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            IngestCoordinatorTestParameters ingestCoordinatorTestParameters,
            TestIngestType ingestType
    ) throws StateStoreException, IteratorException, IOException {
        try (IngestCoordinator<Record> ingestCoordinator =
                     ingestType.createIngestCoordinator(
                             ingestCoordinatorTestParameters)) {
            for (Record record : recordListAndSchema.recordList) {
                ingestCoordinator.write(record);
            }
        }
    }

    private IngestCoordinatorTestParameters.Builder createTestParameterBuilder() {
        return IngestCoordinatorTestParameters
                .builder()
                .temporaryFolder(temporaryFolder)
                .awsResource(AWS_EXTERNAL_RESOURCE)
                .dataBucketName(DATA_BUCKET_NAME);
    }
}
