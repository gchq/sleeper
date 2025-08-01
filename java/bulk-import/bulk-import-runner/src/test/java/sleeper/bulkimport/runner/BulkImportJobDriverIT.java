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
package sleeper.bulkimport.runner;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
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
import sleeper.core.row.Row;
import sleeper.core.row.RowComparator;
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
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.tracker.ingest.job.InMemoryIngestJobTracker;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.ingest.core.job.IngestJob;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.parquet.row.ParquetRowReaderFactory;
import sleeper.parquet.row.ParquetRowWriterFactory;
import sleeper.sketches.store.LocalFileSystemSketchesStore;
import sleeper.sketches.store.SketchesStore;
import sleeper.sketches.testutils.SketchesDeciles;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.commit.SqsFifoStateStoreCommitRequestSender;
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
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
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
    protected final SketchesStore sketchesStore = new LocalFileSystemSketchesStore();

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
        List<Row> rows = getRows();
        writeRowsToFile(rows, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        // - State store
        StateStore stateStore = createTable(instanceProperties, tableProperties);

        // When
        BulkImportJob job = jobForTable(tableProperties).id("my-job").files(inputFiles).build();
        runJob(runner, instanceProperties, job);

        // Then
        List<FileReference> fileReferences = stateStore.getFileReferences();
        assertThat(fileReferences).singleElement().satisfies(fileReference -> {
            SketchesDeciles.fromFile(schema, fileReference, sketchesStore)
                    .equals(SketchesDeciles.builder()
                            .field("key", deciles -> deciles
                                    .min(0).max(99)
                                    .rank(0.1, 10).rank(0.2, 20).rank(0.3, 30)
                                    .rank(0.4, 40).rank(0.5, 50).rank(0.6, 60)
                                    .rank(0.7, 70).rank(0.8, 80).rank(0.9, 90))
                            .build());
        });
        List<Row> readRowss = new ArrayList<>();
        for (FileReference fileReference : fileReferences) {
            List<Row> rowsInThisFile = readRows(fileReference.getFilename(), schema);
            assertThat(rowsInThisFile).isSortedAccordingTo(new RowComparator(getSchema()));
            readRowss.addAll(rowsInThisFile);
        }
        assertThat(readRowss).hasSameSizeAs(rows);

        List<Row> expectedRows = new ArrayList<>(rows);
        sortRows(expectedRows);
        sortRows(readRowss);
        assertThat(readRowss).isEqualTo(expectedRows);
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
        List<Row> rows = getRowsIdenticalRowKey();
        writeRowsToFile(rows, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        // - State store
        StateStore stateStore = createTable(instanceProperties, tableProperties);

        // When
        BulkImportJob job = jobForTable(tableProperties).id("my-job").files(inputFiles).build();
        runJob(runner, instanceProperties, job);

        // Then
        List<FileReference> fileReferences = stateStore.getFileReferences();
        List<Row> readRows = new ArrayList<>();
        for (FileReference fileReference : fileReferences) {
            List<Row> rowsInThisFile = readRows(fileReference.getFilename(), schema);
            assertThat(rowsInThisFile).isSortedAccordingTo(new RowComparator(getSchema()));
            readRows.addAll(rowsInThisFile);
        }
        assertThat(readRows).hasSameSizeAs(rows);

        List<Row> expectedRows = new ArrayList<>(rows);
        sortRows(expectedRows);
        sortRows(readRows);
        assertThat(readRows).isEqualTo(expectedRows);
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
        List<Row> rows = getRows();
        writeRowsToFile(rows, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        // - State store
        StateStore stateStore = createTable(instanceProperties, tableProperties, Collections.singletonList(50));

        // When
        BulkImportJob job = jobForTable(tableProperties).id("my-job").files(inputFiles).build();
        runJob(runner, instanceProperties, job);

        // Then
        List<Row> leftPartition = rows.stream()
                .filter(row -> ((int) row.get("key")) < 50)
                .collect(Collectors.toList());
        sortRows(leftPartition);
        List<Row> rightPartition = rows.stream()
                .filter(row -> ((int) row.get("key")) >= 50)
                .collect(Collectors.toList());
        sortRows(rightPartition);
        assertThat(stateStore.getFileReferences())
                .extracting(FileReference::getNumberOfRows,
                        file -> readRows(file.getFilename(), schema))
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
        List<Row> rows = getLotsOfRows();
        writeRowsToFile(rows, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        // - State store
        StateStore stateStore = createTable(instanceProperties, tableProperties, getSplitPointsForLotsOfRows());

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

            long totalRows = relevantFiles.stream()
                    .map(FileReference::getNumberOfRows)
                    .reduce(Long::sum)
                    .orElseThrow();

            assertThat(totalRows).isEqualTo(2000L);

            relevantFiles.stream()
                    .map(af -> {
                        try {
                            return ParquetRowReaderFactory.createParquetRowReader(new Path(af.getFilename()), schema);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .map(reader -> {
                        List<Row> rowsRead = new ArrayList<>();
                        Row row;
                        try {
                            row = reader.read();
                            while (row != null) {
                                rowsRead.add(row);
                                row = reader.read();
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        return rowsRead;
                    })
                    .forEach(read -> assertThat(read).isSortedAccordingTo(new RowComparator(getSchema())));
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
        List<Row> rows = getRows();
        writeRowsToFile(rows, dataDir + "/import/a.parquet");
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
        sortRows(rows);
        assertThat(stateStore.getFileReferences())
                .extracting(FileReference::getNumberOfRows, FileReference::getPartitionId,
                        file -> readRows(file.getFilename(), schema))
                .containsExactly(tuple(200L, expectedPartitionId, rows));
        IngestJob ingestJob = job.toIngestJob();
        assertThat(tracker.getAllJobs(tableProperties.get(TABLE_ID)))
                .containsExactly(ingestJobStatus(ingestJob, jobRunOnTask(taskId,
                        ingestAcceptedStatus(ingestJob, validationTime),
                        validatedIngestStartedStatus(ingestJob, startTime),
                        ingestFinishedStatus(summary(startTime, endTime, 200, 200), 1))));
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldDeleteJsonFileAfterImport(BulkImportJobRunner runner) throws IOException {
        // Given
        // - Write some data to be imported
        List<Row> rows = getRows();
        writeRowsToFile(rows, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        // - State store
        StateStore stateStore = createTable(instanceProperties, tableProperties);

        // When
        BulkImportJob job = jobForTable(tableProperties).id("my-job").files(inputFiles).build();
        runJob(runner, instanceProperties, job);

        // Then
        List<FileReference> fileReferences = stateStore.getFileReferences();
        List<Row> readRows = new ArrayList<>();
        for (FileReference fileReference : fileReferences) {
            List<Row> rowsInThisFile = readRows(fileReference.getFilename(), schema);
            assertThat(rowsInThisFile).isSortedAccordingTo(new RowComparator(getSchema()));
            readRows.addAll(rowsInThisFile);
        }
        assertThat(readRows).hasSameSizeAs(rows);

        List<Row> expectedRows = new ArrayList<>(rows);
        sortRows(expectedRows);
        sortRows(readRows);
        assertThat(readRows).isEqualTo(expectedRows);
        IngestJob ingestJob = job.toIngestJob();
        assertThat(tracker.getAllJobs(tableProperties.get(TABLE_ID)))
                .containsExactly(ingestJobStatus(ingestJob, jobRunOnTask(taskId,
                        ingestAcceptedStatus(ingestJob, validationTime),
                        validatedIngestStartedStatus(ingestJob, startTime),
                        ingestFinishedStatus(summary(startTime, endTime, 200, 200), 1))));

        // Check json file has been deleted
        assertThat(listObjectKeys(instanceProperties.get(BULK_IMPORT_BUCKET))).isEmpty();
    }

    private static List<Row> readRows(String filename, Schema schema) {
        try (ParquetReader<Row> reader = ParquetRowReaderFactory.createParquetRowReader(new Path(filename), schema)) {
            List<Row> readRows = new ArrayList<>();
            Row row = reader.read();
            while (null != row) {
                readRows.add(new Row(row));
                row = reader.read();
            }
            return readRows;
        } catch (IOException e) {
            throw new RuntimeException("Failed reading rows", e);
        }
    }

    private static void sortRows(List<Row> rows) {
        RowComparator rowComparator = new RowComparator(getSchema());
        rows.sort(rowComparator);
    }

    public InstanceProperties createInstanceProperties(String dir) {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(DATA_BUCKET, dir);
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(BULK_IMPORT_BUCKET, "bulkimport");

        createBucket(instanceProperties.get(CONFIG_BUCKET));
        createBucket(instanceProperties.get(BULK_IMPORT_BUCKET));
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
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

    private static List<Row> getRows() {
        List<Row> rows = new ArrayList<>(200);
        for (int i = 0; i < 100; i++) {
            Row row = new Row();
            row.put("key", i);
            row.put("sort", (long) i);
            row.put("value1", "" + i);
            row.put("value2", Arrays.asList(1, 2, 3));
            Map<String, Long> map = new HashMap<>();
            map.put("A", 1L);
            row.put("value3", map);
            rows.add(row);
            // Add row again but with the sort field set to a different value
            Row row2 = new Row(row);
            row2.put("sort", ((long) row.get("sort")) - 1L);
            rows.add(row2);
        }
        Collections.shuffle(rows);
        return rows;
    }

    private static List<Row> getRowsIdenticalRowKey() {
        List<Row> rows = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            Row row = new Row();
            row.put("key", 1);
            row.put("sort", (long) i);
            row.put("value1", "" + i);
            row.put("value2", Arrays.asList(1, 2, 3));
            Map<String, Long> map = new HashMap<>();
            map.put("A", 1L);
            row.put("value3", map);
            rows.add(row);
        }
        Collections.shuffle(rows);
        return rows;
    }

    private static List<Row> getLotsOfRows() {
        List<Row> rows = new ArrayList<>(100000);
        for (int i = 0; i < 50000; i++) {
            Row row = new Row();
            row.put("key", i);
            row.put("sort", (long) i);
            row.put("value1", "" + i);
            row.put("value2", Arrays.asList(1, 2, 3));
            Map<String, Long> map = new HashMap<>();
            map.put("A", 1L);
            row.put("value3", map);
            rows.add(row);
            // Add row again but with the sort field set to a different value
            Row row2 = new Row(row);
            row2.put("sort", ((long) row.get("sort")) - 1L);
            rows.add(row2);
        }
        Collections.shuffle(rows);
        return rows;
    }

    private static List<Object> getSplitPointsForLotsOfRows() {
        List<Object> splitPoints = new ArrayList<>();
        for (int i = 0; i < 50000; i++) {
            if (i % 1000 == 0) {
                splitPoints.add(i);
            }
        }
        return splitPoints;
    }

    private static void writeRowsToFile(List<Row> rows, String file) throws IllegalArgumentException, IOException {
        ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(new Path(file), getSchema());
        for (Row row : rows) {
            writer.write(row);
        }
        writer.close();
    }

    private StateStore createTable(InstanceProperties instanceProperties, TableProperties tableProperties, List<Object> splitPoints) {
        tablePropertiesStore(instanceProperties).save(tableProperties);
        StateStore stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoClient).getStateStore(tableProperties);
        update(stateStore).initialise(new PartitionsFromSplitPoints(tableProperties.getSchema(), splitPoints).construct());
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
        StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient);
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
