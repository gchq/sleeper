/*
 * Copyright 2022-2026 Crown Copyright
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

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.runner.common.HadoopSketchesStore;
import sleeper.bulkimport.runner.dataframelocalsort.BulkImportDataframeLocalSortDriver;
import sleeper.bulkimport.runner.sketches.GenerateSketchesDriver;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.partition.PartitionTree;
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
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.ingest.tracker.job.DynamoDBIngestJobTrackerCreator;
import sleeper.ingest.tracker.job.IngestJobTrackerFactory;
import sleeper.localstack.test.LocalStackHadoopConfigurationProvider;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.parquet.row.ParquetRowReaderFactory;
import sleeper.parquet.row.ParquetRowWriterFactory;
import sleeper.sketches.store.SketchesStore;
import sleeper.sketches.testutils.SketchesDeciles;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.commit.SqsFifoStateStoreCommitRequestSender;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_MIN_ROWS;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.core.testutils.SupplierTestHelper.supplyNumberedIdsWithPrefix;

public class BulkImportJobDriverLocalStackIT extends LocalStackTestBase {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, getSchema());
    private final SketchesStore sketchesStore = new HadoopSketchesStore(hadoopConf);
    private final String taskId = "test-bulk-import-spark-cluster";
    private final String jobRunId = "test-run";
    private final SparkConf sparkConf = BulkImportSparkContext.createSparkConf();

    @BeforeEach
    public void setup() {
        tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 2);
        tableProperties.setNumber(PARTITION_SPLIT_MIN_ROWS, 100);
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        createBucket(instanceProperties.get(DATA_BUCKET));
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        DynamoDBIngestJobTrackerCreator.create(instanceProperties, dynamoClient);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        tablePropertiesStore().save(tableProperties);
        update(stateStore()).initialise(tableProperties);
        sparkConf.set("spark.master", "local");
        sparkConf.set("spark.app.name", "bulk import");
        LocalStackHadoopConfigurationProvider.configureHadoop(
                (property, value) -> sparkConf.set("spark.hadoop." + property, value),
                localStackContainer);
    }

    @Test
    void shouldImportDataSplittingPartition() throws Exception {
        // Given
        List<Row> rows = getRows();
        writeRowsToFile(rows, s3aPathInDataBucket("import/a.parquet"));
        BulkImportJob job = jobForTable().id("my-job")
                .files(List.of(pathInDataBucket("import/a.parquet")))
                .build();

        // When
        runJob(job);

        // Then
        PartitionTree partitions = new PartitionTree(stateStore().getAllPartitions());
        List<Row> expectedRows = sorted(rows);
        assertThat(partitions.streamLeafPartitions()).hasSize(2);
        assertThat(readFilesInTreeOrder(partitions)).containsExactly(
                new FoundFile(expectedRows.subList(0, 100),
                        SketchesDeciles.builder()
                                .field("key", deciles -> deciles
                                        .min(0).max(49)
                                        .rank(0.1, 5).rank(0.2, 10).rank(0.3, 15)
                                        .rank(0.4, 20).rank(0.5, 25).rank(0.6, 30)
                                        .rank(0.7, 35).rank(0.8, 40).rank(0.9, 45))
                                .build()),
                new FoundFile(expectedRows.subList(100, 200),
                        SketchesDeciles.builder()
                                .field("key", deciles -> deciles
                                        .min(50).max(99)
                                        .rank(0.1, 55).rank(0.2, 60).rank(0.3, 65)
                                        .rank(0.4, 70).rank(0.5, 75).rank(0.6, 80)
                                        .rank(0.7, 85).rank(0.8, 90).rank(0.9, 95))
                                .build()));
    }

    private List<FoundFile> readFilesInTreeOrder(PartitionTree partitions) {
        Map<String, List<String>> partitionIdToFiles = stateStore().getPartitionToReferencedFilesMap();
        return partitions.streamLeavesInTreeOrder()
                .flatMap(partition -> partitionIdToFiles.get(partition.getId()).stream())
                .map(filename -> new FoundFile(readRows(filename), SketchesDeciles.fromFile(tableProperties.getSchema(), filename, sketchesStore)))
                .toList();
    }

    private record FoundFile(List<Row> rows, SketchesDeciles sketches) {
    }

    private void runJob(BulkImportJob job) throws Exception {
        jobTracker().jobValidated(job.toIngestJob().acceptedEventBuilder(Instant.now()).jobRunId(jobRunId).build());
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, tablePropertiesStore());
        StateStoreCommitRequestSender commitSender = new SqsFifoStateStoreCommitRequestSender(
                instanceProperties, sqsClient, s3Client, TransactionSerDeProvider.from(tablePropertiesProvider));
        BulkImportJobDriver<BulkImportSparkContext> driver = new BulkImportJobDriver<>(
                BulkImportSparkContext.creator(instanceProperties, sparkConf), GenerateSketchesDriver::generatePartitionIdToSketches, jobRunner().asImporter(),
                tablePropertiesProvider, stateStoreProvider(), jobTracker(), commitSender, Instant::now, supplyNumberedIdsWithPrefix("P"));
        driver.run(job, jobRunId, taskId);
    }

    private BulkImportJobRunner jobRunner() {
        return BulkImportDataframeLocalSortDriver::createFileReferences;
    }

    private String s3aPathInDataBucket(String path) {
        return "s3a://" + pathInDataBucket(path);
    }

    private String pathInDataBucket(String path) {
        return instanceProperties.get(DATA_BUCKET) + "/" + path;
    }

    private StateStore stateStore() {
        return stateStoreFactory().getStateStore(tableProperties);
    }

    private StateStoreFactory stateStoreFactory() {
        return new StateStoreFactory(instanceProperties, s3Client, dynamoClient);
    }

    private StateStoreProvider stateStoreProvider() {
        return new StateStoreProvider(instanceProperties, stateStoreFactory());
    }

    private TablePropertiesStore tablePropertiesStore() {
        return S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);
    }

    private IngestJobTracker jobTracker() {
        return IngestJobTrackerFactory.getTracker(dynamoClient, instanceProperties);
    }

    private void writeRowsToFile(List<Row> rows, String path) throws IllegalArgumentException, IOException {
        ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(new Path(path), tableProperties.getSchema(), hadoopConf);
        for (Row row : rows) {
            writer.write(row);
        }
        writer.close();
    }

    private BulkImportJob.Builder jobForTable() {
        return BulkImportJob.builder()
                .tableName(tableProperties.get(TABLE_NAME));
    }

    private List<Row> sorted(List<Row> rows) {
        List<Row> sorted = new ArrayList<>(rows);
        sorted.sort(new RowComparator(tableProperties.getSchema()));
        return sorted;
    }

    private List<Row> readRows(String filename) {
        try (ParquetReader<Row> reader = ParquetRowReaderFactory.parquetRowReaderBuilder(
                new Path(filename), tableProperties.getSchema()).withConf(hadoopConf).build()) {
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
            row.put("value2", List.of(1, 2, 3));
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

}
