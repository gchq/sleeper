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
package sleeper.bulkimport.job.runner;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.runner.dataframe.BulkImportJobDataframeDriver;
import sleeper.bulkimport.job.runner.dataframelocalsort.BulkImportDataframeLocalSortDriver;
import sleeper.bulkimport.job.runner.rdd.BulkImportJobRDDDriver;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.record.Record;
import sleeper.core.record.RecordComparator;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.job.status.WriteToMemoryIngestJobStatusStore;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;
import sleeper.statestore.s3.S3StateStore;
import sleeper.statestore.s3.S3StateStoreCreator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.record.process.RecordsProcessedSummaryTestData.summary;
import static sleeper.ingest.job.status.IngestJobStatusTestData.finishedIngestJobWithValidation;
import static sleeper.ingest.job.status.IngestJobValidatedEvent.ingestJobAccepted;

@Testcontainers
class BulkImportJobDriverIT {

    private static Stream<Arguments> getParameters() {
        return Stream.of(
                Arguments.of(Named.of("BulkImportJobDataframeDriver",
                        (BulkImportJobRunner) BulkImportJobDataframeDriver::createFileInfos)),
                Arguments.of(Named.of("BulkImportJobRDDDriver",
                        (BulkImportJobRunner) BulkImportJobRDDDriver::createFileInfos)),
                Arguments.of(Named.of("BulkImportDataframeLocalSortDriver",
                        (BulkImportJobRunner) BulkImportDataframeLocalSortDriver::createFileInfos))
        );
    }

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.S3
    );

    @TempDir
    public java.nio.file.Path folder;
    private final AmazonS3 s3Client = createS3Client();
    private final AmazonDynamoDB dynamoDBClient = createDynamoClient();
    private final Schema schema = getSchema();
    private final IngestJobStatusStore statusStore = new WriteToMemoryIngestJobStatusStore();
    private final String taskId = "test-bulk-import-spark-cluster";
    private final Instant validationTime = Instant.parse("2023-04-05T16:00:01Z");
    private final Instant startTime = Instant.parse("2023-04-05T16:01:01Z");
    private final Instant endTime = Instant.parse("2023-04-05T16:01:11Z");

    @BeforeAll
    public static void setSparkProperties() {
        System.setProperty("spark.master", "local");
        System.setProperty("spark.app.name", "bulk import");
    }

    @AfterAll
    public static void clearSparkProperties() {
        System.clearProperty("spark.master");
        System.clearProperty("spark.app.name");
    }

    private static AmazonS3 createS3Client() {
        return buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    }

    private static AmazonDynamoDB createDynamoClient() {
        return buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    }

    private static List<Record> readRecords(String filename, Schema schema) {
        try (ParquetRecordReader reader = new ParquetRecordReader(new Path(filename), schema)) {
            List<Record> readRecords = new ArrayList<>();
            Record record = reader.read();
            while (null != record) {
                readRecords.add(new Record(record));
                record = reader.read();
            }
            return readRecords;
        } catch (IOException e) {
            throw new RuntimeException("Failed reading records", e);
        }
    }

    private static void sortRecords(List<Record> records) {
        RecordComparator recordComparator = new RecordComparator(getSchema());
        records.sort(recordComparator);
    }

    public InstanceProperties createInstanceProperties(AmazonS3 s3Client, String dir) {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(DATA_BUCKET, dir);
        instanceProperties.set(FILE_SYSTEM, "file://");

        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));
        new DynamoDBStateStoreCreator(instanceProperties, dynamoDBClient).create();
        new S3StateStoreCreator(instanceProperties, dynamoDBClient).create();

        return instanceProperties;
    }

    public TableProperties createTable(InstanceProperties instanceProperties) {
        TableProperties tableProperties = createTableProperties(instanceProperties);
        tableProperties.saveToS3(s3Client);
        return tableProperties;
    }

    public TableProperties createTableProperties(InstanceProperties instanceProperties) {
        return createTestTableProperties(instanceProperties, schema);
    }

    private static Schema getSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .sortKeyFields(new Field("sort", new LongType()))
                .valueFields(
                        new Field("value1", new StringType()),
                        new Field("value2", new ListType(new IntType())),
                        new Field("value3", new MapType(new StringType(), new LongType())))
                .build();
    }

    private static List<Record> getRecords() {
        List<Record> records = new ArrayList<>(200);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", i);
            record.put("sort", (long) i);
            record.put("value1", "" + i);
            record.put("value2", Arrays.asList(1, 2, 3));
            Map<String, Long> map = new HashMap<>();
            map.put("A", 1L);
            record.put("value3", map);
            records.add(record);
            // Add record again but with the sort field set to a different value
            Record record2 = new Record(record);
            record2.put("sort", ((long) record.get("sort")) - 1L);
            records.add(record2);
        }
        Collections.shuffle(records);
        return records;
    }

    private static List<Record> getRecordsIdenticalRowKey() {
        List<Record> records = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", 1);
            record.put("sort", (long) i);
            record.put("value1", "" + i);
            record.put("value2", Arrays.asList(1, 2, 3));
            Map<String, Long> map = new HashMap<>();
            map.put("A", 1L);
            record.put("value3", map);
            records.add(record);
        }
        Collections.shuffle(records);
        return records;
    }

    private static List<Record> getLotsOfRecords() {
        List<Record> records = new ArrayList<>(100000);
        for (int i = 0; i < 50000; i++) {
            Record record = new Record();
            record.put("key", i);
            record.put("sort", (long) i);
            record.put("value1", "" + i);
            record.put("value2", Arrays.asList(1, 2, 3));
            Map<String, Long> map = new HashMap<>();
            map.put("A", 1L);
            record.put("value3", map);
            records.add(record);
            // Add record again but with the sort field set to a different value
            Record record2 = new Record(record);
            record2.put("sort", ((long) record.get("sort")) - 1L);
            records.add(record2);
        }
        Collections.shuffle(records);
        return records;
    }

    private static List<Object> getSplitPointsForLotsOfRecords() {
        List<Object> splitPoints = new ArrayList<>();
        for (int i = 0; i < 50000; i++) {
            if (i % 1000 == 0) {
                splitPoints.add(i);
            }
        }
        return splitPoints;
    }

    private static void writeRecordsToFile(List<Record> records, String file) throws IllegalArgumentException, IOException {
        ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(file), getSchema());
        for (Record record : records) {
            writer.write(record);
        }
        writer.close();
    }

    private static StateStore initialiseStateStore(AmazonDynamoDB dynamoDBClient, InstanceProperties instanceProperties, TableProperties tableProperties, List<Object> splitPoints) throws StateStoreException {
        StateStore stateStore = new DynamoDBStateStore(instanceProperties, tableProperties, dynamoDBClient);
        stateStore.initialise(new PartitionsFromSplitPoints(getSchema(), splitPoints).construct());
        return stateStore;
    }

    private static StateStore initialiseStateStore(AmazonDynamoDB dynamoDBClient, InstanceProperties instanceProperties, TableProperties tableProperties) throws StateStoreException {
        return initialiseStateStore(dynamoDBClient, instanceProperties, tableProperties, Collections.emptyList());
    }

    private static StateStore initialiseS3StateStore(AmazonDynamoDB dynamoDBClient, InstanceProperties instanceProperties, TableProperties tableProperties) throws StateStoreException {
        StateStore stateStore = new S3StateStore(instanceProperties, tableProperties, dynamoDBClient, new Configuration());
        stateStore.initialise();
        return stateStore;
    }

    private void runJob(BulkImportJobRunner runner, InstanceProperties properties, BulkImportJob job) throws IOException {
        String jobRunId = "test-run";
        statusStore.jobValidated(ingestJobAccepted(job.toIngestJob(), validationTime).jobRunId(jobRunId).build());
        BulkImportJobDriver driver = BulkImportJobDriver.from(runner, properties,
                s3Client, dynamoDBClient, new Configuration(), statusStore,
                List.of(startTime, endTime).iterator()::next);
        driver.run(job, jobRunId, taskId);
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldImportDataSinglePartition(BulkImportJobRunner runner) throws IOException, StateStoreException {
        // Given
        //  - Instance and table properties
        String dataDir = folder.toString();
        InstanceProperties instanceProperties = createInstanceProperties(s3Client, dataDir);
        TableProperties tableProperties = createTable(instanceProperties);
        //  - Write some data to be imported
        List<Record> records = getRecords();
        writeRecordsToFile(records, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        //  - State store
        StateStore stateStore = initialiseStateStore(dynamoDBClient, instanceProperties, tableProperties);

        // When
        BulkImportJob job = BulkImportJob.builder().id("my-job").files(inputFiles)
                .tableName(tableProperties.get(TABLE_NAME)).build();
        runJob(runner, instanceProperties, job);

        // Then
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        List<Record> readRecords = new ArrayList<>();
        for (FileInfo fileInfo : activeFiles) {
            try (ParquetRecordReader reader = new ParquetRecordReader(new Path(fileInfo.getFilename()), schema)) {
                List<Record> recordsInThisFile = new ArrayList<>();
                Record record = reader.read();
                while (null != record) {
                    Record clonedRecord = new Record(record);
                    readRecords.add(clonedRecord);
                    recordsInThisFile.add(clonedRecord);
                    record = reader.read();
                }
                assertThat(recordsInThisFile).isSortedAccordingTo(new RecordComparator(getSchema()));
            }
        }
        assertThat(readRecords).hasSameSizeAs(records);

        List<Record> expectedRecords = new ArrayList<>(records);
        sortRecords(expectedRecords);
        sortRecords(readRecords);
        assertThat(readRecords).isEqualTo(expectedRecords);
        assertThat(statusStore.getAllJobs(tableProperties.get(TABLE_NAME))).containsExactly(
                finishedIngestJobWithValidation(job.toIngestJob(), taskId, validationTime,
                        summary(startTime, endTime, records.size(), records.size())));
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldImportDataSinglePartitionIdenticalRowKeyDifferentSortKeys(BulkImportJobRunner runner) throws IOException, StateStoreException {
        // Given
        //  - Instance and table properties
        String dataDir = folder.toString();
        InstanceProperties instanceProperties = createInstanceProperties(s3Client, dataDir);
        TableProperties tableProperties = createTable(instanceProperties);
        //  - Write some data to be imported
        List<Record> records = getRecordsIdenticalRowKey();
        writeRecordsToFile(records, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        //  - State store
        StateStore stateStore = initialiseStateStore(dynamoDBClient, instanceProperties, tableProperties);

        // When
        BulkImportJob job = BulkImportJob.builder().id("my-job").files(inputFiles)
                .tableName(tableProperties.get(TABLE_NAME)).build();
        runJob(runner, instanceProperties, job);

        // Then
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        List<Record> readRecords = new ArrayList<>();
        for (FileInfo fileInfo : activeFiles) {
            try (ParquetRecordReader reader = new ParquetRecordReader(new Path(fileInfo.getFilename()), schema)) {
                List<Record> recordsInThisFile = new ArrayList<>();
                Record record = reader.read();
                while (null != record) {
                    Record clonedRecord = new Record(record);
                    readRecords.add(clonedRecord);
                    recordsInThisFile.add(clonedRecord);
                    record = reader.read();
                }
                assertThat(recordsInThisFile).isSortedAccordingTo(new RecordComparator(getSchema()));
            }
        }
        assertThat(readRecords).hasSameSizeAs(records);

        List<Record> expectedRecords = new ArrayList<>(records);
        sortRecords(expectedRecords);
        sortRecords(readRecords);
        assertThat(readRecords).isEqualTo(expectedRecords);
        assertThat(statusStore.getAllJobs(tableProperties.get(TABLE_NAME))).containsExactly(
                finishedIngestJobWithValidation(job.toIngestJob(), taskId, validationTime,
                        summary(startTime, endTime, records.size(), records.size())));
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldImportDataMultiplePartitions(BulkImportJobRunner runner) throws IOException, StateStoreException {
        // Given
        //  - Instance and table properties
        String dataDir = folder.toString();
        InstanceProperties instanceProperties = createInstanceProperties(s3Client, dataDir);
        TableProperties tableProperties = createTable(instanceProperties);
        //  - Write some data to be imported
        List<Record> records = getRecords();
        writeRecordsToFile(records, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        //  - State store
        StateStore stateStore = initialiseStateStore(dynamoDBClient, instanceProperties, tableProperties, Collections.singletonList(50));

        // When
        BulkImportJob job = BulkImportJob.builder().id("my-job").files(inputFiles)
                .tableName(tableProperties.get(TABLE_NAME)).build();
        runJob(runner, instanceProperties, job);

        // Then
        List<Record> leftPartition = records.stream()
                .filter(record -> ((int) record.get("key")) < 50)
                .collect(Collectors.toList());
        sortRecords(leftPartition);
        List<Record> rightPartition = records.stream()
                .filter(record -> ((int) record.get("key")) >= 50)
                .collect(Collectors.toList());
        sortRecords(rightPartition);
        assertThat(stateStore.getActiveFiles())
                .extracting(FileInfo::getNumberOfRecords,
                        file -> readRecords(file.getFilename(), schema))
                .containsExactlyInAnyOrder(
                        tuple(100L, leftPartition),
                        tuple(100L, rightPartition));
        assertThat(statusStore.getAllJobs(tableProperties.get(TABLE_NAME))).containsExactly(
                finishedIngestJobWithValidation(job.toIngestJob(), taskId, validationTime,
                        summary(startTime, endTime, records.size(), records.size())));
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldImportLargeAmountOfDataMultiplePartitions(BulkImportJobRunner runner) throws IOException, StateStoreException {
        // Given
        //  - Instance and table properties
        String dataDir = folder.toString();
        InstanceProperties instanceProperties = createInstanceProperties(s3Client, dataDir);
        TableProperties tableProperties = createTable(instanceProperties);
        //  - Write some data to be imported
        List<Record> records = getLotsOfRecords();
        writeRecordsToFile(records, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        //  - State store
        StateStore stateStore = initialiseStateStore(dynamoDBClient, instanceProperties, tableProperties, getSplitPointsForLotsOfRecords());

        // When
        BulkImportJob job = BulkImportJob.builder().id("my-job").files(inputFiles)
                .tableName(tableProperties.get(TABLE_NAME)).build();
        runJob(runner, instanceProperties, job);

        // Then
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        List<Partition> leafPartitions = stateStore.getLeafPartitions();
        for (Partition leaf : leafPartitions) {
            Integer minRowKey = (Integer) leaf.getRegion().getRange(schema.getRowKeyFieldNames().get(0)).getMin();
            if (Integer.MIN_VALUE == minRowKey) {
                continue;
            }
            List<FileInfo> relevantFiles = activeFiles.stream()
                    .filter(af -> af.getPartitionId().equals(leaf.getId()))
                    .collect(Collectors.toList());

            long totalRecords = relevantFiles.stream()
                    .map(FileInfo::getNumberOfRecords)
                    .reduce(Long::sum)
                    .orElseThrow();

            assertThat(totalRecords).isEqualTo(2000L);

            relevantFiles.stream()
                    .map(af -> {
                        try {
                            return new ParquetRecordReader(new Path(af.getFilename()), schema);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .map(reader -> {
                        List<Record> recordsRead = new ArrayList<>();
                        Record record;
                        try {
                            record = reader.read();
                            while (record != null) {
                                recordsRead.add(record);
                                record = reader.read();
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        return recordsRead;
                    })
                    .forEach(read -> assertThat(read).isSortedAccordingTo(new RecordComparator(getSchema())));
        }
        assertThat(statusStore.getAllJobs(tableProperties.get(TABLE_NAME))).containsExactly(
                finishedIngestJobWithValidation(job.toIngestJob(), taskId, validationTime,
                        summary(startTime, endTime, records.size(), records.size())));
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldNotThrowExceptionIfProvidedWithDirectoryWhichContainsParquetAndNonParquetFiles(BulkImportJobRunner runner) throws IOException, StateStoreException {
        // Given
        //  - Instance and table properties
        String dataDir = folder.toString();
        InstanceProperties instanceProperties = createInstanceProperties(s3Client, dataDir);
        TableProperties tableProperties = createTable(instanceProperties);
        //  - Write some data to be imported
        List<Record> records = getRecords();
        writeRecordsToFile(records, dataDir + "/import/a.parquet");
        //  - Write a dummy file
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(dataDir + "/import/b.txt", StandardCharsets.UTF_8))) {
            bufferedWriter.append("test");
        }
        //  - State store
        StateStore stateStore = initialiseStateStore(dynamoDBClient, instanceProperties, tableProperties);

        // When
        BulkImportJob job = BulkImportJob.builder().id("my-job").files(Lists.newArrayList(dataDir + "/import/"))
                .tableName(tableProperties.get(TABLE_NAME)).build();
        runJob(runner, instanceProperties, job);

        // Then
        String expectedPartitionId = stateStore.getAllPartitions().get(0).getId();
        sortRecords(records);
        assertThat(stateStore.getActiveFiles())
                .extracting(FileInfo::getNumberOfRecords, FileInfo::getPartitionId,
                        file -> readRecords(file.getFilename(), schema))
                .containsExactly(tuple(200L, expectedPartitionId, records));
        assertThat(statusStore.getAllJobs(tableProperties.get(TABLE_NAME))).containsExactly(
                finishedIngestJobWithValidation(job.toIngestJob(), taskId, validationTime,
                        summary(startTime, endTime, records.size(), records.size())));
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldImportDataWithS3StateStore(BulkImportJobRunner runner) throws IOException, StateStoreException {
        // Given
        //  - Instance and table properties
        String dataDir = folder.toString();
        InstanceProperties instanceProperties = createInstanceProperties(s3Client, dataDir);
        TableProperties tableProperties = createTableProperties(instanceProperties);
        tableProperties.set(STATESTORE_CLASSNAME, S3StateStore.class.getName());
        tableProperties.saveToS3(s3Client);
        //  - Write some data to be imported
        List<Record> records = getRecords();
        writeRecordsToFile(records, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        //  - State store
        StateStore stateStore = initialiseS3StateStore(dynamoDBClient, instanceProperties, tableProperties);

        // When
        BulkImportJob job = BulkImportJob.builder().id("my-job").files(inputFiles)
                .tableName(tableProperties.get(TABLE_NAME)).build();
        runJob(runner, instanceProperties, job);

        // Then
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        List<Record> readRecords = new ArrayList<>();
        for (FileInfo fileInfo : activeFiles) {
            try (ParquetRecordReader reader = new ParquetRecordReader(new Path(fileInfo.getFilename()), schema)) {
                List<Record> recordsInThisFile = new ArrayList<>();
                Record record = reader.read();
                while (null != record) {
                    Record clonedRecord = new Record(record);
                    readRecords.add(clonedRecord);
                    recordsInThisFile.add(clonedRecord);
                    record = reader.read();
                }
                assertThat(recordsInThisFile).isSortedAccordingTo(new RecordComparator(getSchema()));
            }
        }
        assertThat(readRecords).hasSameSizeAs(records);

        List<Record> expectedRecords = new ArrayList<>(records);
        sortRecords(expectedRecords);
        sortRecords(readRecords);
        assertThat(readRecords).isEqualTo(expectedRecords);
        assertThat(statusStore.getAllJobs(tableProperties.get(TABLE_NAME))).containsExactly(
                finishedIngestJobWithValidation(job.toIngestJob(), taskId, validationTime,
                        summary(startTime, endTime, records.size(), records.size())));
    }
}
