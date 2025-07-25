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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.IngestFileWritingStrategy;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.runner.testutils.IngestCoordinatorTestParameters;
import sleeper.ingest.runner.testutils.RowGenerator;
import sleeper.ingest.runner.testutils.TestIngestType;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.sketches.store.LocalFileSystemSketchesStore;
import sleeper.sketches.store.SketchesStore;
import sleeper.sketches.testutils.SketchesDeciles;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.nio.file.Files.createTempDirectory;
import static java.util.stream.LongStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.ArrayListIngestProperty.MAX_IN_MEMORY_BATCH_SIZE;
import static sleeper.core.properties.instance.ArrayListIngestProperty.MAX_ROWS_TO_WRITE_LOCALLY;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.model.IngestFileWritingStrategy.ONE_FILE_PER_LEAF;
import static sleeper.core.properties.model.IngestFileWritingStrategy.ONE_REFERENCE_PER_LEAF;
import static sleeper.core.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.ingest.runner.testutils.IngestCoordinatorTestHelper.accurateFileReferenceBuilder;
import static sleeper.ingest.runner.testutils.IngestCoordinatorTestHelper.accurateSplitFileReference;
import static sleeper.ingest.runner.testutils.ResultVerifier.readMergedRowsFromPartitionDataFiles;
import static sleeper.ingest.runner.testutils.ResultVerifier.readRowsFromPartitionDataFile;
import static sleeper.ingest.runner.testutils.RowGenerator.genericKey1D;
import static sleeper.ingest.runner.testutils.TestIngestType.directWriteBackedByArrowWriteToLocalFile;

public class IngestCoordinatorFileWritingStrategyIT extends LocalStackTestBase {

    @TempDir
    public Path temporaryFolder;

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
    private final String dataBucketName = instanceProperties.get(DATA_BUCKET);
    private final TestIngestType ingestType = directWriteBackedByArrowWriteToLocalFile();
    private final Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
    private StateStore stateStore;
    private final SketchesStore sketchesStore = new LocalFileSystemSketchesStore();

    private void setSchema(Schema schema) {
        tableProperties.setSchema(schema);
        stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoClient).getStateStore(tableProperties);
    }

    @BeforeEach
    public void before() {
        createBucket(dataBucketName);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
    }

    @Nested
    @DisplayName("One file per leaf partition")
    class OneFilePerLeafPartition {
        @BeforeEach
        void setUp() {
            tableProperties.setEnum(INGEST_FILE_WRITING_STRATEGY, ONE_FILE_PER_LEAF);
        }

        @Test
        public void shouldWriteOneFileToRootPartition() throws Exception {
            // Given
            RowGenerator.RowListAndSchema rowListAndSchema = generateStringRows("%09d-%s", range(0, 100));
            setSchema(rowListAndSchema.sleeperSchema);
            PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                    .singlePartition("root").buildTree();
            update(stateStore).initialise(tree.getAllPartitions());
            stateStore.fixFileUpdateTime(stateStoreUpdateTime);
            IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                    .fileNames(List.of("rootFile"))
                    .build();

            // When
            ingestRows(rowListAndSchema, parameters);

            // Then
            List<FileReference> actualFiles = stateStore.getFileReferences();
            FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
            FileReference rootFile = fileReferenceFactory.rootFile(
                    ingestType.getFilePrefix(parameters) + "/data/partition_root/rootFile.parquet", 100L);
            List<Row> allRows = readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema,
                    rootFile, hadoopConf);

            assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
            assertThat(actualFiles).containsExactly(rootFile);
            assertThat(allRows).containsExactlyInAnyOrderElementsOf(rowListAndSchema.rowList);
            assertThat(SketchesDeciles.fromFileReferences(rowListAndSchema.sleeperSchema, actualFiles, sketchesStore))
                    .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
        }

        @Test
        public void shouldWriteOneFileToOneLeafPartition() throws Exception {
            // Given
            RowGenerator.RowListAndSchema rowListAndSchema = generateStringRows("%09d-%s", range(0, 25));
            setSchema(rowListAndSchema.sleeperSchema);
            PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "000000050")
                    .buildTree();
            update(stateStore).initialise(tree.getAllPartitions());
            stateStore.fixFileUpdateTime(stateStoreUpdateTime);
            IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                    .fileNames(List.of("lFile"))
                    .build();

            // When
            ingestRows(rowListAndSchema, parameters);

            // Then
            List<FileReference> actualFiles = stateStore.getFileReferences();
            FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
            FileReference lFile = fileReferenceFactory.partitionFile("L",
                    ingestType.getFilePrefix(parameters) + "/data/partition_L/lFile.parquet", 25L);
            List<Row> allRows = readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema,
                    lFile, hadoopConf);

            assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
            assertThat(actualFiles).containsExactly(lFile);
            assertThat(allRows).containsExactlyInAnyOrderElementsOf(rowListAndSchema.rowList);
            assertThat(SketchesDeciles.fromFileReferences(rowListAndSchema.sleeperSchema, actualFiles, sketchesStore))
                    .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
        }

        @Test
        public void shouldWriteOneFileInEachLeafPartition() throws Exception {
            // Given
            RowGenerator.RowListAndSchema rowListAndSchema = generateStringRows("%09d-%s", range(0, 100));
            setSchema(rowListAndSchema.sleeperSchema);
            PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "000000050")
                    .splitToNewChildren("L", "LL", "LR", "000000020")
                    .splitToNewChildren("R", "RL", "RR", "000000080")
                    .buildTree();
            update(stateStore).initialise(tree.getAllPartitions());
            stateStore.fixFileUpdateTime(stateStoreUpdateTime);
            IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                    .fileNames(List.of("llFile", "lrFile", "rlFile", "rrFile"))
                    .build();

            // When
            ingestRows(rowListAndSchema, parameters);

            // Then
            List<FileReference> actualFiles = stateStore.getFileReferences();
            FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
            FileReference llFile = fileReferenceFactory.partitionFile("LL",
                    ingestType.getFilePrefix(parameters) + "/data/partition_LL/llFile.parquet", 20L);
            FileReference lrFile = fileReferenceFactory.partitionFile("LR",
                    ingestType.getFilePrefix(parameters) + "/data/partition_LR/lrFile.parquet", 30L);
            FileReference rlFile = fileReferenceFactory.partitionFile("RL",
                    ingestType.getFilePrefix(parameters) + "/data/partition_RL/rlFile.parquet", 30L);
            FileReference rrFile = fileReferenceFactory.partitionFile("RR",
                    ingestType.getFilePrefix(parameters) + "/data/partition_RR/rrFile.parquet", 20L);

            List<Row> allRows = readMergedRowsFromPartitionDataFiles(rowListAndSchema.sleeperSchema,
                    List.of(llFile, lrFile, rlFile, rrFile), hadoopConf);

            assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
            assertThat(actualFiles).containsExactlyInAnyOrder(llFile, lrFile, rlFile, rrFile);
            assertThat(allRows).containsExactlyInAnyOrderElementsOf(rowListAndSchema.rowList);
            assertThat(SketchesDeciles.fromFileReferences(rowListAndSchema.sleeperSchema, actualFiles, sketchesStore))
                    .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
        }

        @Test
        public void shouldWriteRowsWhenThereAreMoreThanCanFitInLocalStore() throws Exception {
            // Given
            RowGenerator.RowListAndSchema rowListAndSchema = generateStringRows("%09d-%s", range(0, 20));
            setSchema(rowListAndSchema.sleeperSchema);
            PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "000000010")
                    .buildTree();
            update(stateStore).initialise(tree.getAllPartitions());
            stateStore.fixFileUpdateTime(stateStoreUpdateTime);
            IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                    .fileNames(List.of("leftFile1", "rightFile1", "leftFile2", "rightFile2"))
                    .build();

            // When
            int maxRowsInMemory = 5;
            long maxRowsToWriteToLocalStore = 10L;
            ingestRows(rowListAndSchema, parameters, maxRowsInMemory, maxRowsToWriteToLocalStore);

            // Then
            List<FileReference> actualFiles = stateStore.getFileReferences();
            FileReference leftFile1 = accurateFileReferenceBuilder(
                    ingestType.getFilePrefix(parameters) + "/data/partition_L/leftFile1.parquet", "L", 4L, stateStoreUpdateTime)
                    .onlyContainsDataForThisPartition(true)
                    .build();
            FileReference rightFile1 = accurateFileReferenceBuilder(
                    ingestType.getFilePrefix(parameters) + "/data/partition_R/rightFile1.parquet", "R", 6L, stateStoreUpdateTime)
                    .onlyContainsDataForThisPartition(true)
                    .build();
            FileReference leftFile2 = accurateFileReferenceBuilder(
                    ingestType.getFilePrefix(parameters) + "/data/partition_L/leftFile2.parquet", "L", 6L, stateStoreUpdateTime)
                    .onlyContainsDataForThisPartition(true)
                    .build();
            FileReference rightFile2 = accurateFileReferenceBuilder(
                    ingestType.getFilePrefix(parameters) + "/data/partition_R/rightFile2.parquet", "R", 4L, stateStoreUpdateTime)
                    .onlyContainsDataForThisPartition(true)
                    .build();
            List<Row> allRows = readMergedRowsFromPartitionDataFiles(rowListAndSchema.sleeperSchema,
                    List.of(leftFile1, rightFile1, leftFile2, rightFile2), hadoopConf);

            assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
            assertThat(actualFiles).containsExactlyInAnyOrder(leftFile1, rightFile1, leftFile2, rightFile2);
            assertThat(allRows).containsExactlyInAnyOrderElementsOf(rowListAndSchema.rowList);
            assertThat(SketchesDeciles.fromFileReferences(rowListAndSchema.sleeperSchema, actualFiles, sketchesStore))
                    .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
        }
    }

    @Nested
    @DisplayName("One reference per leaf partition")
    class OneReferencePerLeafPartition {
        @BeforeEach
        void setUp() {
            tableProperties.setEnum(INGEST_FILE_WRITING_STRATEGY, ONE_REFERENCE_PER_LEAF);
        }

        @Test
        public void shouldWriteOneFileToRootPartition() throws Exception {
            // Given
            RowGenerator.RowListAndSchema rowListAndSchema = generateStringRows("%09d-%s", range(0, 100));
            setSchema(rowListAndSchema.sleeperSchema);
            PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                    .singlePartition("root").buildTree();
            update(stateStore).initialise(tree.getAllPartitions());
            stateStore.fixFileUpdateTime(stateStoreUpdateTime);
            IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                    .fileNames(List.of("rootFile"))
                    .build();

            // When
            ingestRows(rowListAndSchema, parameters);

            // Then
            List<FileReference> actualFiles = stateStore.getFileReferences();
            String rootFilename = ingestType.getFilePrefix(parameters) + "/data/partition_root/rootFile.parquet";
            FileReference rootFile = accurateFileReferenceBuilder(rootFilename, "root", 100L, stateStoreUpdateTime)
                    .onlyContainsDataForThisPartition(true)
                    .build();
            List<Row> allRows = readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema,
                    rootFile, hadoopConf);

            assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
            assertThat(actualFiles).containsExactly(rootFile);
            assertThat(allRows).containsExactlyInAnyOrderElementsOf(rowListAndSchema.rowList);
            assertThat(SketchesDeciles.fromFileReferences(rowListAndSchema.sleeperSchema, actualFiles, sketchesStore))
                    .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
        }

        @Test
        public void shouldWriteOneFileWithReferenceInOneLeafPartition() throws Exception {
            // Given
            RowGenerator.RowListAndSchema rowListAndSchema = generateStringRows("%09d-%s", range(0, 25));
            setSchema(rowListAndSchema.sleeperSchema);
            PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "000000050")
                    .buildTree();
            update(stateStore).initialise(tree.getAllPartitions());
            stateStore.fixFileUpdateTime(stateStoreUpdateTime);
            IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                    .fileNames(List.of("rootFile"))
                    .build();

            // When
            ingestRows(rowListAndSchema, parameters);

            // Then
            List<FileReference> actualFiles = stateStore.getFileReferences();
            FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
            String rootFilename = ingestType.getFilePrefix(parameters) + "/data/partition_root/rootFile.parquet";
            FileReference lReference = fileReferenceFactory.partitionFile("L", rootFilename, 25L);

            List<Row> allRows = readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema,
                    lReference, hadoopConf);

            assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
            assertThat(actualFiles).containsExactlyInAnyOrder(lReference);
            assertThat(allRows).containsExactlyInAnyOrderElementsOf(rowListAndSchema.rowList);
            assertThat(SketchesDeciles.fromFileReferences(rowListAndSchema.sleeperSchema, actualFiles, sketchesStore))
                    .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
        }

        @Test
        public void shouldWriteOneFileWithReferencesInLeafPartitions() throws Exception {
            // Given
            RowGenerator.RowListAndSchema rowListAndSchema = generateStringRows("%09d-%s", range(0, 100));
            setSchema(rowListAndSchema.sleeperSchema);
            PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "000000050")
                    .splitToNewChildren("L", "LL", "LR", "000000020")
                    .splitToNewChildren("R", "RL", "RR", "000000080")
                    .buildTree();
            update(stateStore).initialise(tree.getAllPartitions());
            stateStore.fixFileUpdateTime(stateStoreUpdateTime);
            IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                    .fileNames(List.of("rootFile"))
                    .build();

            // When
            ingestRows(rowListAndSchema, parameters);

            // Then
            List<FileReference> actualFiles = stateStore.getFileReferences();
            FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
            String rootFilename = ingestType.getFilePrefix(parameters) + "/data/partition_root/rootFile.parquet";
            FileReference rootFile = fileReferenceFactory.rootFile(rootFilename, 100L);
            FileReference llReference = accurateSplitFileReference(rootFile, "LL", 20L, stateStoreUpdateTime);
            FileReference lrReference = accurateSplitFileReference(rootFile, "LR", 30L, stateStoreUpdateTime);
            FileReference rlReference = accurateSplitFileReference(rootFile, "RL", 30L, stateStoreUpdateTime);
            FileReference rrReference = accurateSplitFileReference(rootFile, "RR", 20L, stateStoreUpdateTime);

            List<Row> allRows = readRowsFromPartitionDataFile(rowListAndSchema.sleeperSchema,
                    rootFile, hadoopConf);

            assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
            assertThat(actualFiles).containsExactlyInAnyOrder(llReference, lrReference, rlReference, rrReference);
            assertThat(allRows).containsExactlyInAnyOrderElementsOf(rowListAndSchema.rowList);
            assertThat(SketchesDeciles.fromFileReferences(rowListAndSchema.sleeperSchema, actualFiles, sketchesStore))
                    .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
        }

        @Test
        public void shouldWriteRowsWhenThereAreMoreThanCanFitInLocalStore() throws Exception {
            // Given
            RowGenerator.RowListAndSchema rowListAndSchema = generateStringRows("%09d-%s", range(0, 20));
            setSchema(rowListAndSchema.sleeperSchema);
            PartitionTree tree = new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "000000010")
                    .buildTree();
            update(stateStore).initialise(tree.getAllPartitions());
            stateStore.fixFileUpdateTime(stateStoreUpdateTime);
            IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                    .fileNames(List.of("rootFile1", "rootFile2"))
                    .build();

            // When
            int maxRowsInMemory = 5;
            long maxRowsToWriteToLocalStore = 10L;
            ingestRows(rowListAndSchema, parameters, maxRowsInMemory, maxRowsToWriteToLocalStore);

            // Then
            List<FileReference> actualFiles = stateStore.getFileReferences();
            String rootFilename1 = ingestType.getFilePrefix(parameters) + "/data/partition_root/rootFile1.parquet";
            FileReference rootFile1 = accurateFileReferenceBuilder(rootFilename1, "root", 10L, stateStoreUpdateTime)
                    .onlyContainsDataForThisPartition(true)
                    .build();
            FileReference leftFile1 = accurateSplitFileReference(rootFile1, "L", 4L, stateStoreUpdateTime);
            FileReference rightFile1 = accurateSplitFileReference(rootFile1, "R", 6L, stateStoreUpdateTime);
            String rootFilename2 = ingestType.getFilePrefix(parameters) + "/data/partition_root/rootFile2.parquet";
            FileReference rootFile2 = accurateFileReferenceBuilder(rootFilename2, "root", 10L, stateStoreUpdateTime)
                    .onlyContainsDataForThisPartition(true)
                    .build();
            FileReference leftFile2 = accurateSplitFileReference(rootFile2, "L", 6L, stateStoreUpdateTime);
            FileReference rightFile2 = accurateSplitFileReference(rootFile2, "R", 4L, stateStoreUpdateTime);
            List<Row> allRows = readMergedRowsFromPartitionDataFiles(rowListAndSchema.sleeperSchema,
                    List.of(rootFile1, rootFile2), hadoopConf);

            assertThat(Paths.get(parameters.getLocalWorkingDir())).isEmptyDirectory();
            assertThat(actualFiles).containsExactly(leftFile1, rightFile1, leftFile2, rightFile2);
            assertThat(allRows).containsExactlyInAnyOrderElementsOf(rowListAndSchema.rowList);
            assertThat(SketchesDeciles.fromFileReferences(rowListAndSchema.sleeperSchema, actualFiles, sketchesStore))
                    .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
        }
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
            IngestCoordinatorTestParameters parameters,
            int maxRowsInMemory,
            long maxRowsToWriteToLocalStore) throws IteratorCreationException, IOException {
        try (IngestCoordinator<Row> ingestCoordinator = parameters.toBuilder()
                .localDirectWrite().backedByArrayList().setInstanceProperties(properties -> {
                    properties.setNumber(MAX_ROWS_TO_WRITE_LOCALLY, maxRowsToWriteToLocalStore);
                    properties.setNumber(MAX_IN_MEMORY_BATCH_SIZE, maxRowsInMemory);
                }).buildCoordinator()) {
            for (Row row : rowListAndSchema.rowList) {
                ingestCoordinator.write(row);
            }
        }
    }

    private static void ingestRows(
            RowGenerator.RowListAndSchema rowListAndSchema,
            IngestCoordinatorTestParameters ingestCoordinatorTestParameters) throws IteratorCreationException, IOException {
        try (IngestCoordinator<Row> ingestCoordinator = directWriteBackedByArrowWriteToLocalFile()
                .createIngestCoordinator(ingestCoordinatorTestParameters)) {
            for (Row row : rowListAndSchema.rowList) {
                ingestCoordinator.write(row);
            }
        }
    }

    private IngestCoordinatorTestParameters.Builder createTestParameterBuilder() throws Exception {
        return IngestCoordinatorTestParameters
                .builder()
                .localDataPath(createTempDirectory(temporaryFolder, null).toString())
                .localWorkingDir(createTempDirectory(temporaryFolder, null).toString())
                .hadoopConfiguration(hadoopConf)
                .dataBucketName(dataBucketName)
                .tableId(tableProperties.get(TABLE_ID))
                .ingestFileWritingStrategy(tableProperties.getEnumValue(INGEST_FILE_WRITING_STRATEGY, IngestFileWritingStrategy.class))
                .schema(tableProperties.getSchema())
                .stateStore(stateStore);
    }

    private static RowGenerator.RowListAndSchema generateStringRows(String formatString, LongStream range) {
        // RandomStringGenerator generates random unicode strings to test both standard and unusual character sets
        Supplier<String> randomString = randomStringGeneratorWithMaxLength(25);
        List<String> keys = range
                .mapToObj(longValue -> String.format(formatString, longValue, randomString.get()))
                .collect(Collectors.toList());
        return genericKey1D(new StringType(), keys);
    }
}
