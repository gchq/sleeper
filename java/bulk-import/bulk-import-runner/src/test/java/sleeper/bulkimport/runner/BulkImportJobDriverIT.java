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
package sleeper.bulkimport.runner;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.runner.dataframe.BulkImportJobDataframeDriver;
import sleeper.bulkimport.runner.dataframelocalsort.BulkImportDataframeLocalSortDriver;
import sleeper.bulkimport.runner.rdd.BulkImportJobRDDDriver;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.record.Record;
import sleeper.core.record.RecordComparator;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.statestore.transactionlog.transactions.TransactionSerDeProvider;
import sleeper.core.tracker.ingest.job.InMemoryIngestJobTracker;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.ingest.core.job.IngestJob;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.parquet.record.ParquetRecordReader;
import sleeper.parquet.record.ParquetRecordWriterFactory;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.commit.SqsFifoStateStoreCommitRequestSender;
import sleeper.statestore.s3.S3StateStore;
import sleeper.statestore.s3.S3StateStoreCreator;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestFinishedStatus;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;
import static sleeper.core.tracker.job.run.JobRunTestData.jobRunOnTask;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.ingestAcceptedStatus;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.ingestJobStatus;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.validatedIngestStartedStatus;

class BulkImportJobDriverIT extends LocalStackTestBase {

    private static Stream<Arguments> getParameters() {
        return Stream.of(
                Arguments.of(Named.of("BulkImportJobDataframeDriver",
                        (BulkImportJobRunner) BulkImportJobDataframeDriver::createFileReferences)),
                Arguments.of(Named.of("BulkImportJobRDDDriver",
                        (BulkImportJobRunner) BulkImportJobRDDDriver::createFileReferences)),
                Arguments.of(Named.of("BulkImportDataframeLocalSortDriver",
                        (BulkImportJobRunner) BulkImportDataframeLocalSortDriver::createFileReferences)));
    }

    @TempDir
    public java.nio.file.Path folder;
    private final Schema schema = getSchema();
    private final IngestJobTracker tracker = new InMemoryIngestJobTracker();
    private final String taskId = "test-bulk-import-spark-cluster";
    private final String jobRunId = "test-run";
    private final Instant validationTime = Instant.parse("2023-04-05T16:00:01Z");
    private final Instant startTime = Instant.parse("2023-04-05T16:01:01Z");
    private final Instant endTime = Instant.parse("2023-04-05T16:01:11Z");
    private InstanceProperties instanceProperties;
    private TableProperties tableProperties;
    private String dataDir;

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

    @BeforeEach
    void setUp() {
        dataDir = folder.toString();
        instanceProperties = createInstanceProperties(dataDir);
        tableProperties = createTableProperties(instanceProperties);
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldImportDataSinglePartition(BulkImportJobRunner runner) throws IOException {
        // Given
        // - Write some data to be imported
        List<Record> records = getRecords();
        writeRecordsToFile(records, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        // - State store
        StateStore stateStore = createTable(instanceProperties, tableProperties);

        // When
        BulkImportJob job = jobForTable(tableProperties).id("my-job").files(inputFiles).build();
        runJob(runner, instanceProperties, job);

        // Then
        List<FileReference> fileReferences = stateStore.getFileReferences();
        List<Record> readRecords = new ArrayList<>();
        for (FileReference fileReference : fileReferences) {
            try (ParquetRecordReader reader = new ParquetRecordReader(new Path(fileReference.getFilename()), schema)) {
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
        IngestJob ingestJob = job.toIngestJob();
        assertThat(tracker.getAllJobs(tableProperties.get(TABLE_ID)))
                .containsExactly(ingestJobStatus(ingestJob, jobRunOnTask(taskId,
                        ingestAcceptedStatus(ingestJob, validationTime),
                        validatedIngestStartedStatus(ingestJob, startTime),
                        ingestFinishedStatus(summary(startTime, endTime, 200, 200), 1))));
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldImportDataSinglePartitionIdenticalRowKeyDifferentSortKeys(BulkImportJobRunner runner) throws IOException {
        // Given
        // - Write some data to be imported
        List<Record> records = getRecordsIdenticalRowKey();
        writeRecordsToFile(records, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        // - State store
        StateStore stateStore = createTable(instanceProperties, tableProperties);

        // When
        BulkImportJob job = jobForTable(tableProperties).id("my-job").files(inputFiles).build();
        runJob(runner, instanceProperties, job);

        // Then
        List<FileReference> fileReferences = stateStore.getFileReferences();
        List<Record> readRecords = new ArrayList<>();
        for (FileReference fileReference : fileReferences) {
            try (ParquetRecordReader reader = new ParquetRecordReader(new Path(fileReference.getFilename()), schema)) {
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
        IngestJob ingestJob = job.toIngestJob();
        assertThat(tracker.getAllJobs(tableProperties.get(TABLE_ID)))
                .containsExactly(ingestJobStatus(ingestJob, jobRunOnTask(taskId,
                        ingestAcceptedStatus(ingestJob, validationTime),
                        validatedIngestStartedStatus(ingestJob, startTime),
                        ingestFinishedStatus(summary(startTime, endTime, 100, 100), 1))));
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldImportDataMultiplePartitions(BulkImportJobRunner runner) throws IOException {
        // Given
        // - Write some data to be imported
        List<Record> records = getRecords();
        writeRecordsToFile(records, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        // - State store
        StateStore stateStore = createTable(instanceProperties, tableProperties, Collections.singletonList(50));

        // When
        BulkImportJob job = jobForTable(tableProperties).id("my-job").files(inputFiles).build();
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
        assertThat(stateStore.getFileReferences())
                .extracting(FileReference::getNumberOfRecords,
                        file -> readRecords(file.getFilename(), schema))
                .containsExactlyInAnyOrder(
                        tuple(100L, leftPartition),
                        tuple(100L, rightPartition));
        IngestJob ingestJob = job.toIngestJob();
        assertThat(tracker.getAllJobs(tableProperties.get(TABLE_ID)))
                .containsExactly(ingestJobStatus(ingestJob, jobRunOnTask(taskId,
                        ingestAcceptedStatus(ingestJob, validationTime),
                        validatedIngestStartedStatus(ingestJob, startTime),
                        ingestFinishedStatus(summary(startTime, endTime, 200, 200), 2))));
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldImportLargeAmountOfDataMultiplePartitions(BulkImportJobRunner runner) throws IOException {
        // Given
        // - Write some data to be imported
        List<Record> records = getLotsOfRecords();
        writeRecordsToFile(records, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        // - State store
        StateStore stateStore = createTable(instanceProperties, tableProperties, getSplitPointsForLotsOfRecords());

        // When
        BulkImportJob job = jobForTable(tableProperties).id("my-job").files(inputFiles).build();
        runJob(runner, instanceProperties, job);

        // Then
        List<FileReference> fileReferences = stateStore.getFileReferences();
        List<Partition> leafPartitions = stateStore.getLeafPartitions();
        for (Partition leaf : leafPartitions) {
            Integer minRowKey = (Integer) leaf.getRegion().getRange(schema.getRowKeyFieldNames().get(0)).getMin();
            if (Integer.MIN_VALUE == minRowKey) {
                continue;
            }
            List<FileReference> relevantFiles = fileReferences.stream()
                    .filter(af -> af.getPartitionId().equals(leaf.getId()))
                    .collect(Collectors.toList());

            long totalRecords = relevantFiles.stream()
                    .map(FileReference::getNumberOfRecords)
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
        IngestJob ingestJob = job.toIngestJob();
        assertThat(tracker.getAllJobs(tableProperties.get(TABLE_ID)))
                .containsExactly(ingestJobStatus(ingestJob, jobRunOnTask(taskId,
                        ingestAcceptedStatus(ingestJob, validationTime),
                        validatedIngestStartedStatus(ingestJob, startTime),
                        ingestFinishedStatus(summary(startTime, endTime, 100000, 100000), 50))));
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldNotThrowExceptionIfProvidedWithDirectoryWhichContainsParquetAndNonParquetFiles(BulkImportJobRunner runner) throws IOException {
        // Given
        // - Write some data to be imported
        List<Record> records = getRecords();
        writeRecordsToFile(records, dataDir + "/import/a.parquet");
        // - Write a dummy file
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(dataDir + "/import/b.txt", StandardCharsets.UTF_8))) {
            bufferedWriter.append("test");
        }
        // - State store
        StateStore stateStore = createTable(instanceProperties, tableProperties);

        // When
        BulkImportJob job = jobForTable(tableProperties).id("my-job")
                .files(Lists.newArrayList(dataDir + "/import/")).build();
        runJob(runner, instanceProperties, job);

        // Then
        String expectedPartitionId = stateStore.getAllPartitions().get(0).getId();
        sortRecords(records);
        assertThat(stateStore.getFileReferences())
                .extracting(FileReference::getNumberOfRecords, FileReference::getPartitionId,
                        file -> readRecords(file.getFilename(), schema))
                .containsExactly(tuple(200L, expectedPartitionId, records));
        IngestJob ingestJob = job.toIngestJob();
        assertThat(tracker.getAllJobs(tableProperties.get(TABLE_ID)))
                .containsExactly(ingestJobStatus(ingestJob, jobRunOnTask(taskId,
                        ingestAcceptedStatus(ingestJob, validationTime),
                        validatedIngestStartedStatus(ingestJob, startTime),
                        ingestFinishedStatus(summary(startTime, endTime, 200, 200), 1))));
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldImportDataWithS3StateStore(BulkImportJobRunner runner) throws IOException {
        // Given
        // - Set state store type
        tableProperties.set(STATESTORE_CLASSNAME, S3StateStore.class.getName());
        // - Write some data to be imported
        List<Record> records = getRecords();
        writeRecordsToFile(records, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        // - State store
        StateStore stateStore = createTable(instanceProperties, tableProperties);

        // When
        BulkImportJob job = jobForTable(tableProperties).id("my-job").files(inputFiles).build();
        runJob(runner, instanceProperties, job);

        // Then
        List<FileReference> fileReferences = stateStore.getFileReferences();
        List<Record> readRecords = new ArrayList<>();
        for (FileReference fileReference : fileReferences) {
            try (ParquetRecordReader reader = new ParquetRecordReader(new Path(fileReference.getFilename()), schema)) {
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
        IngestJob ingestJob = job.toIngestJob();
        assertThat(tracker.getAllJobs(tableProperties.get(TABLE_ID)))
                .containsExactly(ingestJobStatus(ingestJob, jobRunOnTask(taskId,
                        ingestAcceptedStatus(ingestJob, validationTime),
                        validatedIngestStartedStatus(ingestJob, startTime),
                        ingestFinishedStatus(summary(startTime, endTime, 200, 200), 1))));
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

    public InstanceProperties createInstanceProperties(String dir) {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(DATA_BUCKET, dir);
        instanceProperties.set(FILE_SYSTEM, "file://");

        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));
        DynamoDBTableIndexCreator.create(dynamoDBClient, instanceProperties);
        new S3StateStoreCreator(instanceProperties, dynamoDBClient).create();
        new TransactionLogStateStoreCreator(instanceProperties, dynamoDBClient).create();
        return instanceProperties;
    }

    public TableProperties createTableProperties(InstanceProperties instanceProperties) {
        return createTestTableProperties(instanceProperties, schema);
    }

    private TablePropertiesStore tablePropertiesStore(InstanceProperties instanceProperties) {
        return S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);
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

    private StateStore createTable(InstanceProperties instanceProperties, TableProperties tableProperties, List<Object> splitPoints) {
        tablePropertiesStore(instanceProperties).save(tableProperties);
        StateStore stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoClient, HADOOP_CONF).getStateStore(tableProperties);
        stateStore.initialise(new PartitionsFromSplitPoints(getSchema(), splitPoints).construct());
        return stateStore;
    }

    private StateStore createTable(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return createTable(instanceProperties, tableProperties, Collections.emptyList());
    }

    private void runJob(BulkImportJobRunner runner, InstanceProperties properties, BulkImportJob job) throws IOException {
        runJob(runner, instanceProperties, job, startAndEndTime());
    }

    private void runJob(BulkImportJobRunner runner, InstanceProperties properties, BulkImportJob job, Supplier<Instant> timeSupplier) throws IOException {
        tracker.jobValidated(job.toIngestJob().acceptedEventBuilder(validationTime).jobRunId(jobRunId).build());
        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
        StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient, HADOOP_CONF);
        StateStoreCommitRequestSender commitSender = new SqsFifoStateStoreCommitRequestSender(
                properties, sqsClient, s3Client, TransactionSerDeProvider.from(tablePropertiesProvider));
        BulkImportJobDriver driver = new BulkImportJobDriver(new BulkImportSparkSessionRunner(
                runner, instanceProperties, tablePropertiesProvider, stateStoreProvider),
                tablePropertiesProvider, stateStoreProvider, tracker, commitSender, timeSupplier);
        driver.run(job, jobRunId, taskId);
    }

    private Supplier<Instant> startAndEndTime() {
        return List.of(startTime, endTime).iterator()::next;
    }

    private BulkImportJob.Builder jobForTable(TableProperties tableProperties) {
        return BulkImportJob.builder()
                .tableId(tableProperties.get(TABLE_ID))
                .tableName(tableProperties.get(TABLE_NAME));
    }
}
