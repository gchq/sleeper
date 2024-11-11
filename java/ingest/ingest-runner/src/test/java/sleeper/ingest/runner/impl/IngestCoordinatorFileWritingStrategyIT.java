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

package sleeper.ingest.runner.impl;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.core.CommonTestConstants;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.validation.IngestFileWritingStrategy;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.runner.testutils.IngestCoordinatorTestParameters;
import sleeper.ingest.runner.testutils.RecordGenerator;
import sleeper.ingest.runner.testutils.TestIngestType;
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
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.ArrayListIngestProperty.MAX_IN_MEMORY_BATCH_SIZE;
import static sleeper.core.properties.instance.ArrayListIngestProperty.MAX_RECORDS_TO_WRITE_LOCALLY;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.properties.validation.IngestFileWritingStrategy.ONE_FILE_PER_LEAF;
import static sleeper.core.properties.validation.IngestFileWritingStrategy.ONE_REFERENCE_PER_LEAF;
import static sleeper.ingest.runner.testutils.IngestCoordinatorTestHelper.accurateFileReferenceBuilder;
import static sleeper.ingest.runner.testutils.IngestCoordinatorTestHelper.accurateSplitFileReference;
import static sleeper.ingest.runner.testutils.RecordGenerator.genericKey1D;
import static sleeper.ingest.runner.testutils.ResultVerifier.readMergedRecordsFromPartitionDataFiles;
import static sleeper.ingest.runner.testutils.ResultVerifier.readRecordsFromPartitionDataFile;
import static sleeper.ingest.runner.testutils.TestIngestType.directWriteBackedByArrowWriteToLocalFile;
import static sleeper.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
public class IngestCoordinatorFileWritingStrategyIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);
    @TempDir
    public Path temporaryFolder;
    private final Configuration hadoopConfiguration = getHadoopConfiguration(localStackContainer);
    private final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
    private final String dataBucketName = instanceProperties.get(DATA_BUCKET);
    private final TestIngestType ingestType = directWriteBackedByArrowWriteToLocalFile();
    private final Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");

    private StateStore createStateStore(Schema schema) {
        tableProperties.setSchema(schema);
        return new StateStoreFactory(instanceProperties, s3, dynamoDB, hadoopConfiguration).getStateStore(tableProperties);
    }

    @BeforeEach
    public void before() {
        s3.createBucket(dataBucketName);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoDB).create();
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
            RecordGenerator.RecordListAndSchema recordListAndSchema = generateStringRecords("%09d-%s", range(0, 100));
            StateStore stateStore = createStateStore(recordListAndSchema.sleeperSchema);
            PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                    .singlePartition("root").buildTree();
            stateStore.initialise(tree.getAllPartitions());
            stateStore.fixFileUpdateTime(stateStoreUpdateTime);
            String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
            IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                    .fileNames(List.of("rootFile"))
                    .stateStore(stateStore)
                    .schema(recordListAndSchema.sleeperSchema)
                    .workingDir(ingestLocalWorkingDirectory)
                    .build();

            // When
            ingestRecords(recordListAndSchema, parameters);

            // Then
            List<FileReference> actualFiles = stateStore.getFileReferences();
            FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
            FileReference rootFile = fileReferenceFactory.rootFile(
                    ingestType.getFilePrefix(parameters) + "/data/partition_root/rootFile.parquet", 100L);
            List<Record> allRecords = readRecordsFromPartitionDataFile(recordListAndSchema.sleeperSchema,
                    rootFile, hadoopConfiguration);

            assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
            assertThat(actualFiles).containsExactly(rootFile);
            assertThat(allRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
            assertThat(SketchesDeciles.fromFileReferences(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration))
                    .isEqualTo(SketchesDeciles.from(recordListAndSchema.sleeperSchema, recordListAndSchema.recordList));
        }

        @Test
        public void shouldWriteOneFileToOneLeafPartition() throws Exception {
            // Given
            RecordGenerator.RecordListAndSchema recordListAndSchema = generateStringRecords("%09d-%s", range(0, 25));
            StateStore stateStore = createStateStore(recordListAndSchema.sleeperSchema);
            PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "000000050")
                    .buildTree();
            stateStore.initialise(tree.getAllPartitions());
            stateStore.fixFileUpdateTime(stateStoreUpdateTime);
            String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
            IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                    .fileNames(List.of("lFile"))
                    .stateStore(stateStore)
                    .schema(recordListAndSchema.sleeperSchema)
                    .workingDir(ingestLocalWorkingDirectory)
                    .build();

            // When
            ingestRecords(recordListAndSchema, parameters);

            // Then
            List<FileReference> actualFiles = stateStore.getFileReferences();
            FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
            FileReference lFile = fileReferenceFactory.partitionFile("L",
                    ingestType.getFilePrefix(parameters) + "/data/partition_L/lFile.parquet", 25L);
            List<Record> allRecords = readRecordsFromPartitionDataFile(recordListAndSchema.sleeperSchema,
                    lFile, hadoopConfiguration);

            assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
            assertThat(actualFiles).containsExactly(lFile);
            assertThat(allRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
            assertThat(SketchesDeciles.fromFileReferences(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration))
                    .isEqualTo(SketchesDeciles.from(recordListAndSchema.sleeperSchema, recordListAndSchema.recordList));
        }

        @Test
        public void shouldWriteOneFileInEachLeafPartition() throws Exception {
            // Given
            RecordGenerator.RecordListAndSchema recordListAndSchema = generateStringRecords("%09d-%s", range(0, 100));
            StateStore stateStore = createStateStore(recordListAndSchema.sleeperSchema);
            PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "000000050")
                    .splitToNewChildren("L", "LL", "LR", "000000020")
                    .splitToNewChildren("R", "RL", "RR", "000000080")
                    .buildTree();
            stateStore.initialise(tree.getAllPartitions());
            stateStore.fixFileUpdateTime(stateStoreUpdateTime);
            String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
            IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                    .fileNames(List.of("llFile", "lrFile", "rlFile", "rrFile"))
                    .stateStore(stateStore)
                    .schema(recordListAndSchema.sleeperSchema)
                    .workingDir(ingestLocalWorkingDirectory)
                    .build();

            // When
            ingestRecords(recordListAndSchema, parameters);

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

            List<Record> allRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema,
                    List.of(llFile, lrFile, rlFile, rrFile), hadoopConfiguration);

            assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
            assertThat(actualFiles).containsExactlyInAnyOrder(llFile, lrFile, rlFile, rrFile);
            assertThat(allRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
            assertThat(SketchesDeciles.fromFileReferences(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration))
                    .isEqualTo(SketchesDeciles.from(recordListAndSchema.sleeperSchema, recordListAndSchema.recordList));
        }

        @Test
        public void shouldWriteRecordsWhenThereAreMoreRecordsThanCanFitInLocalStore() throws Exception {
            // Given
            RecordGenerator.RecordListAndSchema recordListAndSchema = generateStringRecords("%09d-%s", range(0, 20));
            StateStore stateStore = createStateStore(recordListAndSchema.sleeperSchema);
            PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "000000010")
                    .buildTree();
            stateStore.initialise(tree.getAllPartitions());
            stateStore.fixFileUpdateTime(stateStoreUpdateTime);
            String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
            IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                    .fileNames(List.of("leftFile1", "rightFile1", "leftFile2", "rightFile2"))
                    .stateStore(stateStore)
                    .schema(recordListAndSchema.sleeperSchema)
                    .workingDir(ingestLocalWorkingDirectory)
                    .build();

            // When
            int maxRecordsInMemory = 5;
            long maxRecordsToWriteToLocalStore = 10L;
            ingestRecords(recordListAndSchema, parameters, maxRecordsInMemory, maxRecordsToWriteToLocalStore);

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
            List<Record> allRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema,
                    List.of(leftFile1, rightFile1, leftFile2, rightFile2), hadoopConfiguration);

            assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
            assertThat(actualFiles).containsExactlyInAnyOrder(leftFile1, rightFile1, leftFile2, rightFile2);
            assertThat(allRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
            assertThat(SketchesDeciles.fromFileReferences(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration))
                    .isEqualTo(SketchesDeciles.from(recordListAndSchema.sleeperSchema, recordListAndSchema.recordList));
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
            RecordGenerator.RecordListAndSchema recordListAndSchema = generateStringRecords("%09d-%s", range(0, 100));
            StateStore stateStore = createStateStore(recordListAndSchema.sleeperSchema);
            PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                    .singlePartition("root").buildTree();
            stateStore.initialise(tree.getAllPartitions());
            stateStore.fixFileUpdateTime(stateStoreUpdateTime);
            String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
            IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                    .fileNames(List.of("rootFile"))
                    .stateStore(stateStore)
                    .schema(recordListAndSchema.sleeperSchema)
                    .workingDir(ingestLocalWorkingDirectory)
                    .build();

            // When
            ingestRecords(recordListAndSchema, parameters);

            // Then
            List<FileReference> actualFiles = stateStore.getFileReferences();
            String rootFilename = ingestType.getFilePrefix(parameters) + "/data/partition_root/rootFile.parquet";
            FileReference rootFile = accurateFileReferenceBuilder(rootFilename, "root", 100L, stateStoreUpdateTime)
                    .onlyContainsDataForThisPartition(true)
                    .build();
            List<Record> allRecords = readRecordsFromPartitionDataFile(recordListAndSchema.sleeperSchema,
                    rootFile, hadoopConfiguration);

            assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
            assertThat(actualFiles).containsExactly(rootFile);
            assertThat(allRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
            assertThat(SketchesDeciles.fromFileReferences(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration))
                    .isEqualTo(SketchesDeciles.from(recordListAndSchema.sleeperSchema, recordListAndSchema.recordList));
        }

        @Test
        public void shouldWriteOneFileWithReferenceInOneLeafPartition() throws Exception {
            // Given
            RecordGenerator.RecordListAndSchema recordListAndSchema = generateStringRecords("%09d-%s", range(0, 25));
            StateStore stateStore = createStateStore(recordListAndSchema.sleeperSchema);
            PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "000000050")
                    .buildTree();
            stateStore.initialise(tree.getAllPartitions());
            stateStore.fixFileUpdateTime(stateStoreUpdateTime);
            String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
            IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                    .fileNames(List.of("rootFile"))
                    .stateStore(stateStore)
                    .schema(recordListAndSchema.sleeperSchema)
                    .workingDir(ingestLocalWorkingDirectory)
                    .build();

            // When
            ingestRecords(recordListAndSchema, parameters);

            // Then
            List<FileReference> actualFiles = stateStore.getFileReferences();
            FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
            String rootFilename = ingestType.getFilePrefix(parameters) + "/data/partition_root/rootFile.parquet";
            FileReference lReference = fileReferenceFactory.partitionFile("L", rootFilename, 25L);

            List<Record> allRecords = readRecordsFromPartitionDataFile(recordListAndSchema.sleeperSchema,
                    lReference, hadoopConfiguration);

            assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
            assertThat(actualFiles).containsExactlyInAnyOrder(lReference);
            assertThat(allRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
            assertThat(SketchesDeciles.fromFileReferences(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration))
                    .isEqualTo(SketchesDeciles.from(recordListAndSchema.sleeperSchema, recordListAndSchema.recordList));
        }

        @Test
        public void shouldWriteOneFileWithReferencesInLeafPartitions() throws Exception {
            // Given
            RecordGenerator.RecordListAndSchema recordListAndSchema = generateStringRecords("%09d-%s", range(0, 100));
            StateStore stateStore = createStateStore(recordListAndSchema.sleeperSchema);
            PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "000000050")
                    .splitToNewChildren("L", "LL", "LR", "000000020")
                    .splitToNewChildren("R", "RL", "RR", "000000080")
                    .buildTree();
            stateStore.initialise(tree.getAllPartitions());
            stateStore.fixFileUpdateTime(stateStoreUpdateTime);
            String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
            IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                    .fileNames(List.of("rootFile"))
                    .stateStore(stateStore)
                    .schema(recordListAndSchema.sleeperSchema)
                    .workingDir(ingestLocalWorkingDirectory)
                    .build();

            // When
            ingestRecords(recordListAndSchema, parameters);

            // Then
            List<FileReference> actualFiles = stateStore.getFileReferences();
            FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
            String rootFilename = ingestType.getFilePrefix(parameters) + "/data/partition_root/rootFile.parquet";
            FileReference rootFile = fileReferenceFactory.rootFile(rootFilename, 100L);
            FileReference llReference = accurateSplitFileReference(rootFile, "LL", 20L, stateStoreUpdateTime);
            FileReference lrReference = accurateSplitFileReference(rootFile, "LR", 30L, stateStoreUpdateTime);
            FileReference rlReference = accurateSplitFileReference(rootFile, "RL", 30L, stateStoreUpdateTime);
            FileReference rrReference = accurateSplitFileReference(rootFile, "RR", 20L, stateStoreUpdateTime);

            List<Record> allRecords = readRecordsFromPartitionDataFile(recordListAndSchema.sleeperSchema,
                    rootFile, hadoopConfiguration);

            assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
            assertThat(actualFiles).containsExactlyInAnyOrder(llReference, lrReference, rlReference, rrReference);
            assertThat(allRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
            assertThat(SketchesDeciles.fromFileReferences(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration))
                    .isEqualTo(SketchesDeciles.from(recordListAndSchema.sleeperSchema, recordListAndSchema.recordList));
        }

        @Test
        public void shouldWriteRecordsWhenThereAreMoreRecordsThanCanFitInLocalStore() throws Exception {
            // Given
            RecordGenerator.RecordListAndSchema recordListAndSchema = generateStringRecords("%09d-%s", range(0, 20));
            StateStore stateStore = createStateStore(recordListAndSchema.sleeperSchema);
            PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "000000010")
                    .buildTree();
            stateStore.initialise(tree.getAllPartitions());
            stateStore.fixFileUpdateTime(stateStoreUpdateTime);
            String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
            IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                    .fileNames(List.of("rootFile1", "rootFile2"))
                    .stateStore(stateStore)
                    .schema(recordListAndSchema.sleeperSchema)
                    .workingDir(ingestLocalWorkingDirectory)
                    .build();

            // When
            int maxRecordsInMemory = 5;
            long maxRecordsToWriteToLocalStore = 10L;
            ingestRecords(recordListAndSchema, parameters, maxRecordsInMemory, maxRecordsToWriteToLocalStore);

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
            List<Record> allRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema,
                    List.of(rootFile1, rootFile2), hadoopConfiguration);

            assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
            assertThat(actualFiles).containsExactly(leftFile1, rightFile1, leftFile2, rightFile2);
            assertThat(allRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
            assertThat(SketchesDeciles.fromFileReferences(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration))
                    .isEqualTo(SketchesDeciles.from(recordListAndSchema.sleeperSchema, recordListAndSchema.recordList));
        }
    }

    private static Supplier<String> randomStringGeneratorWithMaxLength(Integer maxLength) {
        Random random = new Random(0);
        RandomStringGenerator randomStringGenerator = new RandomStringGenerator.Builder()
                .usingRandom(random::nextInt)
                .build();
        return () -> randomStringGenerator.generate(random.nextInt(maxLength));
    }

    private static void ingestRecords(
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            IngestCoordinatorTestParameters parameters,
            int maxRecordsInMemory,
            long maxRecordsToWriteToLocalStore) throws StateStoreException, IteratorCreationException, IOException {
        try (IngestCoordinator<Record> ingestCoordinator = parameters.toBuilder()
                .localDirectWrite().backedByArrayList().setInstanceProperties(properties -> {
                    properties.setNumber(MAX_RECORDS_TO_WRITE_LOCALLY, maxRecordsToWriteToLocalStore);
                    properties.setNumber(MAX_IN_MEMORY_BATCH_SIZE, maxRecordsInMemory);
                }).buildCoordinator()) {
            for (Record record : recordListAndSchema.recordList) {
                ingestCoordinator.write(record);
            }
        }
    }

    private static void ingestRecords(
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            IngestCoordinatorTestParameters ingestCoordinatorTestParameters) throws StateStoreException, IteratorCreationException, IOException {
        try (IngestCoordinator<Record> ingestCoordinator = directWriteBackedByArrowWriteToLocalFile()
                .createIngestCoordinator(ingestCoordinatorTestParameters)) {
            for (Record record : recordListAndSchema.recordList) {
                ingestCoordinator.write(record);
            }
        }
    }

    private IngestCoordinatorTestParameters.Builder createTestParameterBuilder() {
        return IngestCoordinatorTestParameters
                .builder()
                .temporaryFolder(temporaryFolder)
                .hadoopConfiguration(hadoopConfiguration)
                .dataBucketName(dataBucketName)
                .tableId(tableProperties.get(TABLE_ID))
                .ingestFileWritingStrategy(tableProperties.getEnumValue(INGEST_FILE_WRITING_STRATEGY, IngestFileWritingStrategy.class));
    }

    private static RecordGenerator.RecordListAndSchema generateStringRecords(String formatString, LongStream range) {
        // RandomStringGenerator generates random unicode strings to test both standard and unusual character sets
        Supplier<String> randomString = randomStringGeneratorWithMaxLength(25);
        List<String> keys = range
                .mapToObj(longValue -> String.format(formatString, longValue, randomString.get()))
                .collect(Collectors.toList());
        return genericKey1D(new StringType(), keys);
    }
}
