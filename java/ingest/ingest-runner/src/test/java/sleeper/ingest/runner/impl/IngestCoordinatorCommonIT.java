/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.ingest.runner.impl;

import org.apache.commons.text.RandomStringGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.IngestFileWritingStrategy;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.example.iterator.AdditionIterator;
import sleeper.ingest.runner.testutils.IngestCoordinatorTestParameters;
import sleeper.ingest.runner.testutils.ResultVerifier;
import sleeper.ingest.runner.testutils.RowGenerator;
import sleeper.ingest.runner.testutils.TestIngestType;
import sleeper.ingest.runner.testutils.TestIngestType.WriteTarget;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.sketches.store.LocalFileSystemSketchesStore;
import sleeper.sketches.store.S3SketchesStore;
import sleeper.sketches.store.SketchesStore;
import sleeper.sketches.testutils.SketchesDeciles;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.model.IngestFileWritingStrategy.ONE_FILE_PER_LEAF;
import static sleeper.core.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.ingest.runner.testutils.ResultVerifier.readMergedRowsFromPartitionDataFiles;
import static sleeper.ingest.runner.testutils.ResultVerifier.readRowsFromPartitionDataFile;
import static sleeper.ingest.runner.testutils.RowGenerator.genericKey1D;

public class IngestCoordinatorCommonIT extends LocalStackTestBase {

    @TempDir
    public Path temporaryFolder;

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
    private final String dataBucketName = instanceProperties.get(DATA_BUCKET);
    private StateStore stateStore;

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
                        TestIngestType.directWriteBackedByArrayListWriteToS3())));
    }

    @BeforeEach
    public void before() {
        createBucket(dataBucketName);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        tableProperties.setEnum(INGEST_FILE_WRITING_STRATEGY, ONE_FILE_PER_LEAF);
    }

    private void setSchema(Schema schema) {
        tableProperties.setSchema(schema);
        stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoClient).getStateStore(tableProperties);
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRows(TestIngestType ingestType) throws Exception {
        // Given
        RowGenerator.RowListAndSchema rowListAndSchema = genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        setSchema(rowListAndSchema.sleeperSchema);
        PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();
        update(stateStore).initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        stateStore.fixFileUpdateTime(stateStoreUpdateTime);
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("rootFile"))
                .build();

        // When
        ingestRows(rowListAndSchema, parameters, ingestType);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Row> actualRows = readMergedRowsFromPartitionDataFiles(rowListAndSchema.sleeperSchema, actualFiles, hadoopConf);
        FileReference fileReference = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime)
                .rootFile(ingestType.getFilePrefix(parameters) + "/data/partition_root/rootFile.parquet", 200);

        assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
        assertThat(actualFiles).containsExactly(fileReference);
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(rowListAndSchema.rowList);
        assertThat(actualRows).extracting(row -> row.getValues(List.of("key0")))
                .containsExactlyElementsOf(LongStream.range(-100, 100).boxed()
                        .map(List::<Object>of)
                        .collect(Collectors.toList()));
        assertThat(readSketchesDeciles(rowListAndSchema.sleeperSchema, actualFiles, ingestType))
                .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRowsSplitByPartitionIntKey(TestIngestType ingestType) throws Exception {
        // Given
        RowGenerator.RowListAndSchema rowListAndSchema = genericKey1D(
                new IntType(),
                IntStream.range(-100, 100).boxed().collect(Collectors.toList()));
        setSchema(rowListAndSchema.sleeperSchema);
        PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 2)
                .buildTree();
        update(stateStore).initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        stateStore.fixFileUpdateTime(stateStoreUpdateTime);
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .build();

        // When
        ingestRows(rowListAndSchema, parameters, ingestType);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
        FileReference leftFile = fileReferenceFactory.partitionFile("left",
                ingestType.getFilePrefix(parameters) + "/data/partition_left/leftFile.parquet", 102);
        FileReference rightFile = fileReferenceFactory.partitionFile("right",
                ingestType.getFilePrefix(parameters) + "/data/partition_right/rightFile.parquet", 98);
        List<Row> leftRows = readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema, leftFile, hadoopConf);
        List<Row> rightRows = readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema, rightFile, hadoopConf);
        List<Row> allRows = Stream.of(leftRows, rightRows)
                .flatMap(List::stream).collect(Collectors.toUnmodifiableList());

        assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrder(leftFile, rightFile);
        assertThat(allRows).containsExactlyInAnyOrderElementsOf(rowListAndSchema.rowList);
        assertThat(leftRows).extracting(row -> row.getValues(List.of("key0")).get(0))
                .containsExactly(IntStream.range(-100, 2).boxed().toArray());
        assertThat(rightRows).extracting(row -> row.getValues(List.of("key0")).get(0))
                .containsExactly(IntStream.range(2, 100).boxed().toArray());
        assertThat(readSketchesDeciles(rowListAndSchema.sleeperSchema, actualFiles, ingestType))
                .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRowsSplitByPartitionLongKey(TestIngestType ingestType) throws Exception {
        // Given
        RowGenerator.RowListAndSchema rowListAndSchema = genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        setSchema(rowListAndSchema.sleeperSchema);
        PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 2L)
                .buildTree();
        update(stateStore).initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        stateStore.fixFileUpdateTime(stateStoreUpdateTime);
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .build();

        // When
        ingestRows(rowListAndSchema, parameters, ingestType);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
        FileReference leftFile = fileReferenceFactory.partitionFile("left", ingestType.getFilePrefix(parameters) + "/data/partition_left/leftFile.parquet", 102);
        FileReference rightFile = fileReferenceFactory.partitionFile("right", ingestType.getFilePrefix(parameters) + "/data/partition_right/rightFile.parquet", 98);
        List<Row> leftRows = readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema, leftFile, hadoopConf);
        List<Row> rightRows = readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema, rightFile, hadoopConf);
        List<Row> allRows = Stream.of(leftRows, rightRows)
                .flatMap(List::stream).collect(Collectors.toUnmodifiableList());

        assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrder(leftFile, rightFile);
        assertThat(allRows).containsExactlyInAnyOrderElementsOf(rowListAndSchema.rowList);
        assertThat(leftRows).extracting(row -> row.getValues(List.of("key0")).get(0))
                .containsExactly(LongStream.range(-100, 2).boxed().toArray());
        assertThat(rightRows).extracting(row -> row.getValues(List.of("key0")).get(0))
                .containsExactly(LongStream.range(2, 100).boxed().toArray());
        assertThat(readSketchesDeciles(rowListAndSchema.sleeperSchema, actualFiles, ingestType))
                .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRowsSplitByPartitionStringKey(TestIngestType ingestType) throws Exception {
        // Given
        // RandomStringGenerator generates random unicode strings to test both standard and unusual character sets
        Supplier<String> randomString = randomStringGeneratorWithMaxLength(25);
        List<String> keys = LongStream.range(0, 200)
                .mapToObj(longValue -> String.format("%09d-%s", longValue, randomString.get()))
                .collect(Collectors.toList());
        RowGenerator.RowListAndSchema rowListAndSchema = genericKey1D(new StringType(), keys);
        setSchema(rowListAndSchema.sleeperSchema);
        PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", "000000102")
                .buildTree();
        update(stateStore).initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        stateStore.fixFileUpdateTime(stateStoreUpdateTime);
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .build();

        // When
        ingestRows(rowListAndSchema, parameters, ingestType);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
        FileReference leftFile = fileReferenceFactory.partitionFile("left",
                ingestType.getFilePrefix(parameters) + "/data/partition_left/leftFile.parquet", 102);
        FileReference rightFile = fileReferenceFactory.partitionFile("right",
                ingestType.getFilePrefix(parameters) + "/data/partition_right/rightFile.parquet", 98);
        List<Row> leftRows = readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema, leftFile, hadoopConf);
        List<Row> rightRows = readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema, rightFile, hadoopConf);
        List<Row> allRows = Stream.of(leftRows, rightRows)
                .flatMap(List::stream).collect(Collectors.toUnmodifiableList());

        assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrder(leftFile, rightFile);
        assertThat(allRows).containsExactlyInAnyOrderElementsOf(rowListAndSchema.rowList);
        assertThat(leftRows).extracting(row -> row.getValues(List.of("key0")).get(0))
                .containsExactlyElementsOf(keys.subList(0, 102));
        assertThat(rightRows).extracting(row -> row.getValues(List.of("key0")).get(0))
                .containsExactlyElementsOf(keys.subList(102, 200));
        assertThat(readSketchesDeciles(rowListAndSchema.sleeperSchema, actualFiles, ingestType))
                .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRowsSplitByPartitionByteArrayKey(TestIngestType ingestType) throws Exception {
        // Given
        RowGenerator.RowListAndSchema rowListAndSchema = genericKey1D(
                new ByteArrayType(),
                Arrays.asList(
                        new byte[]{1, 1},
                        new byte[]{2, 2},
                        new byte[]{64, 65}));
        setSchema(rowListAndSchema.sleeperSchema);
        PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", new byte[]{64, 64})
                .buildTree();
        update(stateStore).initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        stateStore.fixFileUpdateTime(stateStoreUpdateTime);
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .build();

        // When
        ingestRows(rowListAndSchema, parameters, ingestType);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
        FileReference leftFile = fileReferenceFactory.partitionFile("left",
                ingestType.getFilePrefix(parameters) + "/data/partition_left/leftFile.parquet", 2);
        FileReference rightFile = fileReferenceFactory.partitionFile("right",
                ingestType.getFilePrefix(parameters) + "/data/partition_right/rightFile.parquet", 1);
        List<Row> leftRows = readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema, leftFile, hadoopConf);
        List<Row> rightRows = readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema, rightFile, hadoopConf);
        List<Row> allRows = Stream.of(leftRows, rightRows)
                .flatMap(List::stream).collect(Collectors.toUnmodifiableList());

        assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrder(leftFile, rightFile);
        assertThat(allRows).containsExactlyInAnyOrderElementsOf(rowListAndSchema.rowList);
        assertThat(leftRows).extracting(row -> row.getValues(List.of("key0")).get(0))
                .containsExactly(new byte[]{1, 1}, new byte[]{2, 2});
        assertThat(rightRows).extracting(row -> row.getValues(List.of("key0")).get(0))
                .containsExactly(new byte[]{64, 65});
        assertThat(readSketchesDeciles(rowListAndSchema.sleeperSchema, actualFiles, ingestType))
                .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRowsSplitByPartitionStringKeyLongSortKey(TestIngestType ingestType) throws Exception {
        // Given
        // RandomStringGenerator generates random unicode strings to test both standard and unusual character sets
        Supplier<String> randomString = randomStringGeneratorWithMaxLength(25);
        List<String> stringKeys = LongStream.range(0, 200)
                .mapToObj(longValue -> String.format("%09d-%s", longValue, randomString.get()))
                .flatMap(str -> Stream.of(str, str, str))
                .collect(Collectors.toList());
        List<Long> longKeys = LongStream.range(-100, 100).boxed()
                .flatMap(longValue -> Stream.of(longValue - 1000L, longValue, longValue + 1000L))
                .collect(Collectors.toList());
        RowGenerator.RowListAndSchema rowListAndSchema = RowGenerator.genericKey1DSort1D(
                new StringType(),
                new LongType(),
                stringKeys, longKeys);
        setSchema(rowListAndSchema.sleeperSchema);
        PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "left", "right", 0, "000000102")
                .buildTree();
        update(stateStore).initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        stateStore.fixFileUpdateTime(stateStoreUpdateTime);
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .build();

        // When
        ingestRows(rowListAndSchema, parameters, ingestType);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
        FileReference leftFile = fileReferenceFactory.partitionFile("left",
                ingestType.getFilePrefix(parameters) + "/data/partition_left/leftFile.parquet", 306);
        FileReference rightFile = fileReferenceFactory.partitionFile("right",
                ingestType.getFilePrefix(parameters) + "/data/partition_right/rightFile.parquet", 294);
        List<Row> leftRows = readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema, leftFile, hadoopConf);
        List<Row> rightRows = readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema, rightFile, hadoopConf);
        List<Row> allRows = Stream.of(leftRows, rightRows)
                .flatMap(List::stream).collect(Collectors.toUnmodifiableList());

        assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrder(leftFile, rightFile);
        assertThat(allRows).containsExactlyInAnyOrderElementsOf(rowListAndSchema.rowList);
        assertThat(leftRows).extracting(row -> row.getValues(List.of("key0")).get(0))
                .containsExactlyElementsOf(stringKeys.subList(0, 306));
        assertThat(leftRows).extracting(row -> row.getValues(List.of("sortKey0")).get(0))
                .containsExactlyElementsOf(longKeys.subList(0, 306));
        assertThat(rightRows).extracting(row -> row.getValues(List.of("key0")).get(0))
                .containsExactlyElementsOf(stringKeys.subList(306, 600));
        assertThat(rightRows).extracting(row -> row.getValues(List.of("sortKey0")).get(0))
                .containsExactlyElementsOf(longKeys.subList(306, 600));
        assertThat(readSketchesDeciles(rowListAndSchema.sleeperSchema, actualFiles, ingestType))
                .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRowsSplitByPartition2DimensionalByteArrayKey(TestIngestType ingestType) throws Exception {
        // Given
        RowGenerator.RowListAndSchema rowListAndSchema = RowGenerator.genericKey2D(
                new ByteArrayType(), new ByteArrayType(),
                Arrays.asList(new byte[]{1, 1}, new byte[]{11, 2}, new byte[]{64, 65}, new byte[]{5}),
                Arrays.asList(new byte[]{2, 3}, new byte[]{2, 2}, new byte[]{67, 68}, new byte[]{99}));
        setSchema(rowListAndSchema.sleeperSchema);
        PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "left", "right", 0, new byte[]{10})
                .buildTree();
        update(stateStore).initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        stateStore.fixFileUpdateTime(stateStoreUpdateTime);
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .build();

        // When
        ingestRows(rowListAndSchema, parameters, ingestType);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
        FileReference leftFile = fileReferenceFactory.partitionFile("left",
                ingestType.getFilePrefix(parameters) + "/data/partition_left/leftFile.parquet", 2);
        FileReference rightFile = fileReferenceFactory.partitionFile("right",
                ingestType.getFilePrefix(parameters) + "/data/partition_right/rightFile.parquet", 2);
        List<Row> leftRows = readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema, leftFile, hadoopConf);
        List<Row> rightRows = readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema, rightFile, hadoopConf);
        List<Row> allRows = Stream.of(leftRows, rightRows)
                .flatMap(List::stream).collect(Collectors.toUnmodifiableList());

        assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrder(leftFile, rightFile);
        assertThat(allRows).containsExactlyInAnyOrderElementsOf(rowListAndSchema.rowList);
        assertThat(leftRows)
                .extracting(row -> row.getValues(List.of("key0")).get(0))
                .containsExactly(new byte[]{1, 1}, new byte[]{5});
        assertThat(leftRows)
                .extracting(row -> row.getValues(List.of("key1")).get(0))
                .containsExactly(new byte[]{2, 3}, new byte[]{99});
        assertThat(rightRows)
                .extracting(row -> row.getValues(List.of("key0")).get(0))
                .containsExactly(new byte[]{11, 2}, new byte[]{64, 65});
        assertThat(rightRows)
                .extracting(row -> row.getValues(List.of("key1")).get(0))
                .containsExactly(new byte[]{2, 2}, new byte[]{67, 68});
        assertThat(readSketchesDeciles(rowListAndSchema.sleeperSchema, actualFiles, ingestType))
                .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRowsSplitByPartition2DimensionalIntLongKeyWhenSplitOnDim1(TestIngestType ingestType) throws Exception {
        // Given
        RowGenerator.RowListAndSchema rowListAndSchema = RowGenerator.genericKey2D(
                new IntType(), new LongType(),
                Arrays.asList(0, 0, 100, 100),
                Arrays.asList(1L, 20L, 1L, 50L));
        setSchema(rowListAndSchema.sleeperSchema);
        PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "left", "right", 1, 10L)
                .buildTree();
        update(stateStore).initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        stateStore.fixFileUpdateTime(stateStoreUpdateTime);
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .build();

        // When
        ingestRows(rowListAndSchema, parameters, ingestType);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
        FileReference rightFile = fileReferenceFactory.partitionFile("right", ingestType.getFilePrefix(parameters) +
                "/data/partition_right/rightFile.parquet", 2);
        FileReference leftFile = fileReferenceFactory.partitionFile("left", ingestType.getFilePrefix(parameters) +
                "/data/partition_left/leftFile.parquet", 2);
        List<Row> leftRows = ResultVerifier.readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema, leftFile, hadoopConf);
        List<Row> rightRows = ResultVerifier.readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema, rightFile, hadoopConf);
        List<Row> allRows = Stream.of(leftRows, rightRows)
                .flatMap(List::stream).collect(Collectors.toUnmodifiableList());

        assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrder(leftFile, rightFile);
        assertThat(allRows).containsExactlyInAnyOrderElementsOf(rowListAndSchema.rowList);
        assertThat(leftRows)
                .extracting(row -> row.getValues(List.of("key0", "key1")))
                .containsExactly(List.of(0, 1L), List.of(100, 1L));
        assertThat(rightRows)
                .extracting(row -> row.getValues(List.of("key0", "key1")))
                .containsExactly(List.of(0, 20L), List.of(100, 50L));
        assertThat(readSketchesDeciles(rowListAndSchema.sleeperSchema, actualFiles, ingestType))
                .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRowsSplitByPartition2DimensionalLongStringKeyWhenSplitOnDim1(TestIngestType ingestType) throws Exception {
        // Given
        RowGenerator.RowListAndSchema rowListAndSchema = RowGenerator.genericKey2D(
                new LongType(), new StringType(),
                LongStream.range(-100L, 100L).boxed().collect(Collectors.toList()),
                LongStream.range(-100L, 100L).mapToObj(Long::toString).collect(Collectors.toList()));
        setSchema(rowListAndSchema.sleeperSchema);
        PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "left", "right", 1, "2")
                .buildTree();
        update(stateStore).initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        stateStore.fixFileUpdateTime(stateStoreUpdateTime);
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .build();

        // When
        ingestRows(rowListAndSchema, parameters, ingestType);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
        FileReference leftFile = fileReferenceFactory.partitionFile("left", ingestType.getFilePrefix(parameters) +
                "/data/partition_left/leftFile.parquet", 112);
        FileReference rightFile = fileReferenceFactory.partitionFile("right", ingestType.getFilePrefix(parameters) +
                "/data/partition_right/rightFile.parquet", 88);
        List<Row> leftRows = ResultVerifier.readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema, leftFile, hadoopConf);
        List<Row> rightRows = ResultVerifier.readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema, rightFile, hadoopConf);
        List<Row> allRows = Stream.of(leftRows, rightRows)
                .flatMap(List::stream).collect(Collectors.toUnmodifiableList());

        assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrder(leftFile, rightFile);
        assertThat(allRows).containsExactlyInAnyOrderElementsOf(rowListAndSchema.rowList);
        assertThat(leftRows)
                .extracting(row -> row.getValues(List.of("key0", "key1")))
                .containsExactlyElementsOf(LongStream.concat(LongStream.range(-100L, 2L), LongStream.range(10L, 20L))
                        .boxed()
                        .map(x -> List.<Object>of(x, String.valueOf(x)))
                        .collect(Collectors.toList()));
        assertThat(rightRows)
                .extracting(row -> row.getValues(List.of("key0", "key1")))
                .containsExactlyElementsOf(LongStream.concat(LongStream.range(2L, 10L), LongStream.range(20L, 100L))
                        .boxed()
                        .map(x -> List.<Object>of(x, String.valueOf(x)))
                        .collect(Collectors.toList()));
        assertThat(readSketchesDeciles(rowListAndSchema.sleeperSchema, actualFiles, ingestType))
                .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteRowsSplitByPartitionWhenThereIsOnlyDataInOnePartition(TestIngestType ingestType) throws Exception {
        // Given
        RowGenerator.RowListAndSchema rowListAndSchema = genericKey1D(
                new LongType(),
                Arrays.asList(1L, 0L));
        setSchema(rowListAndSchema.sleeperSchema);
        PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 2L)
                .buildTree();
        update(stateStore).initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        stateStore.fixFileUpdateTime(stateStoreUpdateTime);
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .build();

        // When
        ingestRows(rowListAndSchema, parameters, ingestType);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Row> actualRows = readMergedRowsFromPartitionDataFiles(rowListAndSchema.sleeperSchema, actualFiles, hadoopConf);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
        FileReference expectedFile = fileReferenceFactory.partitionFile("left",
                ingestType.getFilePrefix(parameters) + "/data/partition_left/leftFile.parquet", 2);
        assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
        assertThat(actualFiles).containsExactly(expectedFile);
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(rowListAndSchema.rowList);
        assertThat(actualRows).extracting(row -> row.getValues(List.of("key0")))
                .containsExactly(List.of(0L), List.of(1L));
        assertThat(readSketchesDeciles(rowListAndSchema.sleeperSchema, actualFiles, ingestType))
                .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteDuplicateRows(
            TestIngestType ingestType) throws Exception {
        // Given
        RowGenerator.RowListAndSchema rowListAndSchema = genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        RowGenerator.RowListAndSchema duplicatedrowListAndSchema = new RowGenerator.RowListAndSchema(
                Stream.of(rowListAndSchema.rowList, rowListAndSchema.rowList)
                        .flatMap(List::stream)
                        .collect(Collectors.toList()),
                rowListAndSchema.sleeperSchema);
        setSchema(rowListAndSchema.sleeperSchema);
        PartitionTree tree = new PartitionsBuilder(duplicatedrowListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();
        update(stateStore).initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        stateStore.fixFileUpdateTime(stateStoreUpdateTime);
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .build();

        // When
        ingestRows(duplicatedrowListAndSchema, parameters, ingestType);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Row> actualRows = readMergedRowsFromPartitionDataFiles(duplicatedrowListAndSchema.sleeperSchema, actualFiles, hadoopConf);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
        FileReference expectedFile = fileReferenceFactory.rootFile(ingestType.getFilePrefix(parameters) + "/data/partition_root/leftFile.parquet", 400);
        assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
        assertThat(actualFiles).containsExactly(expectedFile);
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(duplicatedrowListAndSchema.rowList);
        assertThat(actualRows).extracting(row -> row.getValues(List.of("key0")).get(0))
                .containsExactlyElementsOf(LongStream.range(-100, 100).boxed()
                        .flatMap(longValue -> Stream.of(longValue, longValue))
                        .collect(Collectors.toList()));
        assertThat(readSketchesDeciles(rowListAndSchema.sleeperSchema, actualFiles, ingestType))
                .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldWriteNoRows(TestIngestType ingestType) throws Exception {
        // Given
        RowGenerator.RowListAndSchema rowListAndSchema = genericKey1D(
                new LongType(),
                Collections.emptyList());
        setSchema(rowListAndSchema.sleeperSchema);
        PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();
        update(stateStore).initialise(tree.getAllPartitions());
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of())
                .build();

        // When
        ingestRows(rowListAndSchema, parameters, ingestType);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Row> actualRows = readMergedRowsFromPartitionDataFiles(rowListAndSchema.sleeperSchema, actualFiles, hadoopConf);
        assertThat(actualFiles).isEmpty();
        assertThat(actualRows).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("parameterObjsForTests")
    public void shouldApplyIterator(TestIngestType ingestType) throws Exception {
        // Given
        RowGenerator.RowListAndSchema rowListAndSchema = RowGenerator.byteArrayRowKeyLongSortKey(
                Arrays.asList(new byte[]{1, 1}, new byte[]{1, 1}, new byte[]{11, 12}, new byte[]{11, 12}),
                Arrays.asList(1L, 1L, 2L, 2L),
                Arrays.asList(1L, 2L, 3L, 4L));
        List<Row> expectedRows = List.of(
                new Row(Map.of(
                        "key", new byte[]{1, 1},
                        "sort", 1L,
                        "value", 3L)),
                new Row(Map.of(
                        "key", new byte[]{11, 12},
                        "sort", 2L,
                        "value", 7L)));
        setSchema(rowListAndSchema.sleeperSchema);
        PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                .rootFirst("root")
                .buildTree();
        update(stateStore).initialise(tree.getAllPartitions());
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        stateStore.fixFileUpdateTime(stateStoreUpdateTime);
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("rootFile"))
                .iteratorClassName(AdditionIterator.class.getName())
                .build();

        // When
        ingestRows(rowListAndSchema, parameters, ingestType);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Row> actualRows = readMergedRowsFromPartitionDataFiles(rowListAndSchema.sleeperSchema, actualFiles, hadoopConf);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
        FileReference expectedFile = fileReferenceFactory.rootFile(ingestType.getFilePrefix(parameters) + "/data/partition_root/rootFile.parquet", 2);
        assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
        assertThat(actualFiles).containsExactly(expectedFile);
        assertThat(actualRows).containsExactlyElementsOf(expectedRows);
        assertThat(readSketchesDeciles(rowListAndSchema.sleeperSchema, actualFiles, ingestType))
                .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, expectedRows));
    }

    private static Supplier<String> randomStringGeneratorWithMaxLength(Integer maxLength) {
        Random random = new Random(0);
        RandomStringGenerator randomStringGenerator = new RandomStringGenerator.Builder()
                .usingRandom(random::nextInt)
                .build();
        return () -> randomStringGenerator.generate(random.nextInt(maxLength));
    }

    private static void ingestRows(
            RowGenerator.RowListAndSchema rowListAndSchema,
            IngestCoordinatorTestParameters ingestCoordinatorTestParameters,
            TestIngestType ingestType) throws IteratorCreationException, IOException {
        try (IngestCoordinator<Row> ingestCoordinator = ingestType.createIngestCoordinator(
                ingestCoordinatorTestParameters)) {
            for (Row row : rowListAndSchema.rowList) {
                ingestCoordinator.write(row);
            }
        }
    }

    private SketchesDeciles readSketchesDeciles(Schema schema, List<FileReference> fileReferences, TestIngestType ingestType) {
        return SketchesDeciles.fromFileReferences(schema, fileReferences, getSketchesStore(ingestType));
    }

    private SketchesStore getSketchesStore(TestIngestType ingestType) {
        if (ingestType.getWriteTarget() == WriteTarget.S3) {
            return new S3SketchesStore(s3Client, s3TransferManager);
        } else {
            return new LocalFileSystemSketchesStore();
        }
    }

    private IngestCoordinatorTestParameters.Builder createTestParameterBuilder() throws Exception {
        return IngestCoordinatorTestParameters.builder()
                .localDataPath(createTempDirectory(temporaryFolder, null).toString())
                .localWorkingDir(createTempDirectory(temporaryFolder, null).toString())
                .hadoopConfiguration(hadoopConf)
                .s3AsyncClient(s3AsyncClient)
                .dataBucketName(dataBucketName)
                .tableId(tableProperties.get(TABLE_ID))
                .ingestFileWritingStrategy(tableProperties.getEnumValue(INGEST_FILE_WRITING_STRATEGY, IngestFileWritingStrategy.class))
                .schema(tableProperties.getSchema())
                .stateStore(stateStore);
    }
}
