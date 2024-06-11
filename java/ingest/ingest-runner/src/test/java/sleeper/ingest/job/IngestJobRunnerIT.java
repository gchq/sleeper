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
package sleeper.ingest.job;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.CommonTestConstants;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.IngestResult;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.ingest.testutils.ResultVerifier;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.statestore.FixedStateStoreProvider;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_RECORD_BATCH_TYPE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;
import static sleeper.ingest.testutils.LocalStackAwsV2ClientHelper.buildAwsV2Client;
import static sleeper.ingest.testutils.ResultVerifier.readMergedRecordsFromPartitionDataFiles;
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
class IngestJobRunnerIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    protected final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    protected final S3AsyncClient s3Async = buildAwsV2Client(localStackContainer, LocalStackContainer.Service.S3, S3AsyncClient.builder());
    protected final Configuration hadoopConfiguration = getHadoopConfiguration(localStackContainer);

    private final String instanceId = UUID.randomUUID().toString().substring(0, 18);
    private final String tableName = UUID.randomUUID().toString();
    private final String ingestDataBucketName = tableName + "-ingestdata";
    private final String tableDataBucketName = tableName + "-tabledata";
    @TempDir
    public java.nio.file.Path temporaryFolder;
    private String currentLocalIngestDirectory;
    private String currentLocalTableDataDirectory;

    private static Stream<Arguments> parametersForTests() {
        return Stream.of(
                Arguments.of("arrow", "async", "s3a://"),
                Arguments.of("arrow", "direct", "s3a://"),
                Arguments.of("arrow", "direct", "file://"),
                Arguments.of("arraylist", "async", "s3a://"),
                Arguments.of("arraylist", "direct", "s3a://"),
                Arguments.of("arraylist", "direct", "file://"));
    }

    @BeforeEach
    public void before() throws IOException {
        s3.createBucket(tableDataBucketName);
        s3.createBucket(ingestDataBucketName);
        currentLocalIngestDirectory = createTempDirectory(temporaryFolder, null).toString();
        currentLocalTableDataDirectory = createTempDirectory(temporaryFolder, null).toString();
    }

    private InstanceProperties getInstanceProperties(String fileSystemPrefix,
            String recordBatchType,
            String partitionFileWriterType) {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, instanceId);
        instanceProperties.set(FILE_SYSTEM, fileSystemPrefix);
        instanceProperties.set(DATA_BUCKET, getTableDataBucket(fileSystemPrefix));
        instanceProperties.set(DEFAULT_INGEST_RECORD_BATCH_TYPE, recordBatchType);
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, partitionFileWriterType);
        return instanceProperties;
    }

    private TableProperties createTable(Schema schema, String fileSystemPrefix,
            String recordBatchType,
            String partitionFileWriterType) {
        InstanceProperties instanceProperties = getInstanceProperties(fileSystemPrefix, recordBatchType, partitionFileWriterType);
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.setSchema(schema);
        return tableProperties;
    }

    private String getTableDataBucket(String fileSystemPrefix) {
        switch (fileSystemPrefix.toLowerCase(Locale.ROOT)) {
            case "s3a://":
                return tableDataBucketName;
            case "file://":
                return currentLocalTableDataDirectory;
            default:
                throw new AssertionError(String.format("File system %s is not supported", fileSystemPrefix));
        }
    }

    private String getIngestBucket(String fileSystemPrefix) {
        switch (fileSystemPrefix.toLowerCase(Locale.ROOT)) {
            case "s3a://":
                return ingestDataBucketName;
            case "file://":
                return currentLocalIngestDirectory;
            default:
                throw new AssertionError(String.format("File system %s is not supported", fileSystemPrefix));
        }
    }

    private List<String> writeParquetFilesForIngest(String fileSystemPrefix,
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            String subDirectory,
            int numberOfFiles) throws IOException {
        List<String> files = new ArrayList<>();

        for (int fileNo = 0; fileNo < numberOfFiles; fileNo++) {
            String fileWithoutSystemPrefix = String.format("%s/%s/file-%d.parquet",
                    getIngestBucket(fileSystemPrefix), subDirectory, fileNo);
            files.add(fileWithoutSystemPrefix);
            Path path = new Path(fileSystemPrefix + fileWithoutSystemPrefix);
            writeParquetFileForIngest(path, recordListAndSchema);
        }

        return files;
    }

    private void writeParquetFileForIngest(
            Path path, RecordGenerator.RecordListAndSchema recordListAndSchema) throws IOException {
        ParquetWriter<Record> writer = ParquetRecordWriterFactory
                .createParquetRecordWriter(path, recordListAndSchema.sleeperSchema, hadoopConfiguration);
        for (Record record : recordListAndSchema.recordList) {
            writer.write(record);
        }
        writer.close();
    }

    @ParameterizedTest(name = "backedBy: {0}, writeMode: {1}, fileSystem: {2}")
    @MethodSource("parametersForTests")
    void shouldIngestParquetFiles(String recordBatchType,
            String partitionFileWriterType,
            String fileSystemPrefix) throws Exception {
        // Given
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-5, 5).boxed().collect(Collectors.toList()));
        String localDir = createTempDirectory(temporaryFolder, null).toString();
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(recordListAndSchema.sleeperSchema);

        List<String> files = writeParquetFilesForIngest(
                fileSystemPrefix, recordListAndSchema, "", 2);
        List<Record> doubledRecords = Stream.of(recordListAndSchema.recordList, recordListAndSchema.recordList)
                .flatMap(List::stream).collect(Collectors.toList());

        // When
        runIngestJobAndCommitResult(
                stateStore,
                fileSystemPrefix,
                recordBatchType,
                partitionFileWriterType,
                recordListAndSchema,
                localDir,
                files);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        assertThat(Paths.get(localDir)).isEmptyDirectory();
        assertThat(actualFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile("anyfilename", 20));
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(doubledRecords);
        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getField("key0").orElseThrow(),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration);
    }

    @ParameterizedTest(name = "backedBy: {0}, writeMode: {1}, fileSystem: {2}")
    @MethodSource("parametersForTests")
    void shouldBeAbleToHandleAllFileFormats(String recordBatchType,
            String partitionFileWriterType,
            String fileSystemPrefix) throws Exception {
        // Given
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        List<String> files = writeParquetFilesForIngest(
                fileSystemPrefix, recordListAndSchema, "", 1);
        String ingestBucket = getIngestBucket(fileSystemPrefix);
        URI uri1 = new URI(fileSystemPrefix + ingestBucket + "/file-1.crc");
        FileSystem.get(uri1, hadoopConfiguration).createNewFile(new Path(uri1));
        files.add(ingestBucket + "/file-1.crc");
        URI uri2 = new URI(fileSystemPrefix + ingestBucket + "/file-2.csv");
        FileSystem.get(uri2, hadoopConfiguration).createNewFile(new Path(uri2));
        files.add(ingestBucket + "/file-2.csv");
        String localDir = createTempDirectory(temporaryFolder, null).toString();
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(recordListAndSchema.sleeperSchema);

        // When
        runIngestJobAndCommitResult(
                stateStore,
                fileSystemPrefix,
                recordBatchType,
                partitionFileWriterType,
                recordListAndSchema,
                localDir,
                files);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        assertThat(Paths.get(localDir)).isEmptyDirectory();
        assertThat(actualFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile("anyfilename", 200));
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getField("key0").orElseThrow(),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration);
    }

    @ParameterizedTest(name = "backedBy: {0}, writeMode: {1}, fileSystem:{2}")
    @MethodSource("parametersForTests")
    void shouldIngestParquetFilesInNestedDirectories(String recordBatchType,
            String partitionFileWriterType,
            String fileSystemPrefix) throws Exception {
        // Given
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-5, 5).boxed().collect(Collectors.toList()));
        int noOfTopLevelDirectories = 2;
        int noOfNestings = 4;
        int noOfFilesPerDirectory = 2;
        List<String> files = IntStream.range(0, noOfTopLevelDirectories)
                .mapToObj(topLevelDirNo -> IntStream.range(0, noOfNestings).mapToObj(nestingNo -> {
                    try {
                        String dirName = String.format("dir-%d%s", topLevelDirNo, String.join("", Collections.nCopies(nestingNo, "/nested-dir")));
                        return writeParquetFilesForIngest(
                                fileSystemPrefix, recordListAndSchema, dirName, noOfFilesPerDirectory);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).flatMap(List::stream).collect(Collectors.toList()))
                .flatMap(List::stream).collect(Collectors.toList());
        List<Record> expectedRecords = Collections.nCopies(noOfTopLevelDirectories * noOfNestings * noOfFilesPerDirectory, recordListAndSchema.recordList).stream()
                .flatMap(List::stream).collect(Collectors.toList());
        String localDir = createTempDirectory(temporaryFolder, null).toString();
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(recordListAndSchema.sleeperSchema);

        // When
        runIngestJobAndCommitResult(
                stateStore,
                fileSystemPrefix,
                recordBatchType,
                partitionFileWriterType,
                recordListAndSchema,
                localDir,
                files);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        assertThat(Paths.get(localDir)).isEmptyDirectory();
        assertThat(actualFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile("anyfilename", 160));
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);
        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getField("key0").orElseThrow(),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration);
    }

    @Test
    void shouldWriteRecordsFromTwoBuckets() throws Exception {
        // Given
        RecordGenerator.RecordListAndSchema records1 = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-5, 5).boxed().collect(Collectors.toList()));
        RecordGenerator.RecordListAndSchema records2 = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(10, 20).boxed().collect(Collectors.toList()));
        writeParquetFileForIngest(new Path("s3a://" + tableDataBucketName + "/ingest/file1.parquet"), records1);
        writeParquetFileForIngest(new Path("s3a://" + ingestDataBucketName + "/ingest/file2.parquet"), records2);

        IngestJob ingestJob = IngestJob.builder()
                .tableName(tableName).id("id").files(
                        tableDataBucketName + "/ingest/file1.parquet",
                        ingestDataBucketName + "/ingest/file2.parquet")
                .build();
        List<Record> expectedRecords = new ArrayList<>();
        expectedRecords.addAll(records1.recordList);
        expectedRecords.addAll(records2.recordList);
        String localDir = createTempDirectory(temporaryFolder, null).toString();
        InstanceProperties instanceProperties = getInstanceProperties("s3a://", "arrow", "async");
        TableProperties tableProperties = createTable(records1.sleeperSchema, "s3a://", "arrow", "async");
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(records1.sleeperSchema);
        IngestJobRunner jobRunner = new IngestJobRunner(
                new ObjectFactory(instanceProperties, null, createTempDirectory(temporaryFolder, null).toString()),
                instanceProperties,
                new FixedTablePropertiesProvider(tableProperties),
                PropertiesReloader.neverReload(),
                new FixedStateStoreProvider(tableProperties, stateStore),
                localDir,
                s3Async,
                hadoopConfiguration);

        // When
        runIngestJobAndCommitResult(stateStore, jobRunner, ingestJob);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(records1.sleeperSchema, actualFiles, hadoopConfiguration);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(stateStore,
                actualFiles.get(0).getLastStateStoreUpdateTime());
        assertThat(Paths.get(localDir)).isEmptyDirectory();
        assertThat(actualFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile("anyfilename", 20));
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);
        ResultVerifier.assertOnSketch(
                new Field("key0", new LongType()),
                new RecordGenerator.RecordListAndSchema(expectedRecords, records1.sleeperSchema),
                actualFiles,
                hadoopConfiguration);
    }

    private void runIngestJobAndCommitResult(
            StateStore stateStore,
            String fileSystemPrefix,
            String recordBatchType,
            String partitionFileWriterType,
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            String localDir,
            List<String> files) throws Exception {
        InstanceProperties instanceProperties = getInstanceProperties(fileSystemPrefix, recordBatchType, partitionFileWriterType);
        TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(createTable(recordListAndSchema.sleeperSchema, fileSystemPrefix, recordBatchType, partitionFileWriterType));
        StateStoreProvider stateStoreProvider = new FixedStateStoreProvider(tablePropertiesProvider.getByName(tableName), stateStore);
        IngestJobRunner jobRunner = new IngestJobRunner(
                new ObjectFactory(instanceProperties, null, createTempDirectory(temporaryFolder, null).toString()),
                instanceProperties,
                tablePropertiesProvider,
                PropertiesReloader.neverReload(),
                stateStoreProvider,
                localDir,
                s3Async,
                hadoopConfiguration);
        IngestJob job = IngestJob.builder()
                .tableName(tableName)
                .id("id")
                .files(files)
                .build();
        runIngestJobAndCommitResult(stateStore, jobRunner, job);
    }

    private void runIngestJobAndCommitResult(StateStore stateStore, IngestJobRunner jobRunner, IngestJob job) throws Exception {
        IngestResult result = jobRunner.ingest(job);
        stateStore.addFiles(result.getFileReferenceList());
    }
}
