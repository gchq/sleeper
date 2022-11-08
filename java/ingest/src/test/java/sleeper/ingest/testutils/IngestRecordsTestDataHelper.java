/*
 * Copyright 2022 Crown Copyright
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

package sleeper.ingest.testutils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.range.Region;
import sleeper.core.record.CloneRecord;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.ingest.IngestProperties;
import sleeper.ingest.impl.IngestCoordinatorFactory;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreProvider;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ARROW_INGEST_BATCH_BUFFER_BYTES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ARROW_INGEST_WORKING_BUFFER_BYTES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_RECORD_BATCH_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAX_IN_MEMORY_BATCH_SIZE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAX_RECORDS_TO_WRITE_LOCALLY;
import static sleeper.configuration.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.configuration.properties.table.TableProperty.ROW_GROUP_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class IngestRecordsTestDataHelper {
    private IngestRecordsTestDataHelper() {
    }


    public static IngestProperties.Builder defaultPropertiesBuilder(
            StateStore stateStore, Schema sleeperSchema, String ingestLocalWorkingDirectory, String sketchDirectory) {
        return IngestProperties.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(ingestLocalWorkingDirectory)
                .maxRecordsToWriteLocally(10L)
                .maxInMemoryBatchSize(1000L)
                .rowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                .pageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .compressionCodec("zstd")
                .stateStore(stateStore)
                .schema(sleeperSchema)
                .filePathPrefix(sketchDirectory)
                .ingestPartitionRefreshFrequencyInSecond(120);
    }

    public static Schema schemaWithRowKeys(Field... fields) {
        return Schema.builder()
                .rowKeyFields(fields)
                .valueFields(new Field("value1", new LongType()), new Field("value2", new LongType()))
                .build();
    }

    public static Partition createRootPartition(Region region, PrimitiveType... rowKeyTypes) {
        return Partition.builder()
                .rowKeyTypes(rowKeyTypes)
                .id("root")
                .region(region)
                .leafPartition(false)
                .parentPartitionId(null)
                .build();
    }

    public static Partition createLeafPartition(String id, Region region, PrimitiveType... rowKeyTypes) {
        return Partition.builder()
                .rowKeyTypes(rowKeyTypes)
                .id(id)
                .region(region)
                .leafPartition(true)
                .parentPartitionId("root")
                .childPartitionIds(Collections.emptyList())
                .build();
    }

    public static List<Record> getSingleRecord() {
        Record record1 = new Record();
        record1.put("key", 1L);
        record1.put("value1", 2L);
        record1.put("value2", 3L);
        return Collections.singletonList(record1);
    }

    public static List<Record> getRecords() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("key", 1L);
        record1.put("value1", 2L);
        record1.put("value2", 3L);
        Record record2 = new Record();
        record2.put("key", 3L);
        record2.put("value1", 4L);
        record2.put("value2", 6L);
        records.add(record1);
        records.add(record2);
        return records;
    }

    public static List<Record> getLotsOfRecords() {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            Record record1 = new Record();
            record1.put("key", 1L - i);
            record1.put("value1", 2L * i);
            record1.put("value2", 3L * i);
            Record record2 = new Record();
            record2.put("key", 2L + i);
            record2.put("value1", 4L * i);
            record2.put("value2", 6L * i);
            records.add(record1);
            records.add(record2);
        }
        return records;
    }

    public static List<Record> getRecordsInFirstPartitionOnly() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("key", 1L);
        record1.put("value1", 2L);
        record1.put("value2", 3L);
        Record record2 = new Record();
        record2.put("key", 0L);
        record2.put("value1", 4L);
        record2.put("value2", 6L);
        records.add(record1);
        records.add(record2);
        return records;
    }

    public static List<Record> getRecordsByteArrayKey() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("key", new byte[]{1, 1});
        record1.put("value1", 2L);
        record1.put("value2", 3L);
        Record record2 = new Record();
        record2.put("key", new byte[]{2, 2});
        record2.put("value1", 2L);
        record2.put("value2", 3L);
        Record record3 = new Record();
        record3.put("key", new byte[]{64, 65});
        record3.put("value1", 4L);
        record3.put("value2", 6L);
        records.add(record1);
        records.add(record2);
        records.add(record3);
        return records;
    }

    public static List<Record> getRecords2DimByteArrayKey() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("key1", new byte[]{1, 1});
        record1.put("key2", new byte[]{2, 3});
        record1.put("value1", 2L);
        record1.put("value2", 3L);
        Record record2 = new Record();
        record2.put("key1", new byte[]{11, 2});
        record2.put("key2", new byte[]{2, 2});
        record2.put("value1", 2L);
        record2.put("value2", 3L);
        Record record3 = new Record();
        record3.put("key1", new byte[]{64, 65});
        record3.put("key2", new byte[]{67, 68});
        record3.put("value1", 4L);
        record3.put("value2", 6L);
        Record record4 = new Record();
        record4.put("key1", new byte[]{5});
        record4.put("key2", new byte[]{99});
        record4.put("value1", 2L);
        record4.put("value2", 3L);
        records.add(record1);
        records.add(record2);
        records.add(record3);
        records.add(record3); // Add twice so that one file has more entries so we can tell them apart
        records.add(record4);
        return records;
    }

    public static List<Record> getUnsortedRecords() {
        List<Record> records = new ArrayList<>();
        for (int i = 10; i > 0; i--) {
            Record record1 = new Record();
            record1.put("key", (long) i);
            record1.put("value1", 2L);
            record1.put("value2", 3L);
            records.add(record1);
            Record record2 = new Record();
            record2.put("key", 5L);
            record2.put("value1", 4L);
            record2.put("value2", 6L);
            records.add(record2);
        }
        return records;
    }

    public static List<Record> getRecordsForAggregationIteratorTest() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("key", new byte[]{1, 1});
        record1.put("sort", 2L);
        record1.put("value", 1L);
        Record record2 = new Record();
        record2.put("key", new byte[]{11, 2});
        record2.put("sort", 1L);
        record2.put("value", 1L);
        Record record3 = new Record();
        record3.put("key", new byte[]{1, 1});
        record3.put("sort", 2L);
        record3.put("value", 6L);
        Record record4 = new Record();
        record4.put("key", new byte[]{11, 2});
        record4.put("sort", 1L);
        record4.put("value", 3L);
        records.add(record1);
        records.add(record2);
        records.add(record3);
        records.add(record4);
        return records;
    }

    public static List<Record> getRecordsOscillatingBetween2Partitions() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("key1", 0);
        record1.put("key2", 1L);
        record1.put("value1", 2L);
        record1.put("value2", 1L);
        Record record2 = new Record();
        record2.put("key1", 0);
        record2.put("key2", 20L);
        record2.put("value1", 200L);
        record2.put("value2", 100L);
        Record record3 = new Record();
        record3.put("key1", 100);
        record3.put("key2", 1L);
        record3.put("value1", 20000L);
        record3.put("value2", 10000L);
        Record record4 = new Record();
        record4.put("key1", 100);
        record4.put("key2", 50L);
        record4.put("value1", 2000000L);
        record4.put("value2", 1000000L);
        records.add(record1);
        records.add(record2);
        records.add(record3);
        records.add(record4);
        return records;
    }

    public static List<Record> readRecordsFromParquetFile(String filename, Schema schema) throws IOException {
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(filename), schema).build();
        List<Record> readRecords = new ArrayList<>();
        CloneRecord cloneRecord = new CloneRecord(schema);
        Record record = reader.read();
        while (null != record) {
            readRecords.add(cloneRecord.clone(record));
            record = reader.read();
        }
        reader.close();
        return readRecords;
    }

    public static Sketches getSketches(Schema schema, String filename) throws IOException {
        String sketchFile = filename.replace(".parquet", ".sketches");
        assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
        return new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
    }

    public static InstanceProperties createInstanceProperties(String instanceName, String fileSystemPrefix, String recordBatchType, String partitionFileWriterType) {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, instanceName);
        instanceProperties.set(FILE_SYSTEM, fileSystemPrefix);
        instanceProperties.set(INGEST_RECORD_BATCH_TYPE, recordBatchType);
        instanceProperties.set(INGEST_PARTITION_FILE_WRITER_TYPE, partitionFileWriterType);
        instanceProperties.setNumber(INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS, Integer.MAX_VALUE);
        instanceProperties.setNumber(ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS, 10);
        instanceProperties.setNumber(ARROW_INGEST_WORKING_BUFFER_BYTES, 16 * 1024 * 1024L);
        instanceProperties.setNumber(ARROW_INGEST_BATCH_BUFFER_BYTES, 16 * 1024 * 1024L);
        instanceProperties.setNumber(ARROW_INGEST_BATCH_BUFFER_BYTES, 16 * 1024 * 1024L);
        instanceProperties.setNumber(MAX_RECORDS_TO_WRITE_LOCALLY, 128);
        instanceProperties.setNumber(MAX_IN_MEMORY_BATCH_SIZE, 100000L);
        return instanceProperties;
    }

    public static TableProperties createTableProperties(InstanceProperties instanceProperties, Schema schema, String bucketName) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "test-table");
        tableProperties.setSchema(schema);
        tableProperties.set(DATA_BUCKET, bucketName);
        tableProperties.set(COMPRESSION_CODEC, "zstd");
        tableProperties.setNumber(PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE);
        tableProperties.setNumber(ROW_GROUP_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE);
        return tableProperties;
    }

    public static Configuration defaultHadoopConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper");
        conf.set("fs.s3a.fast.upload", "true");
        return conf;
    }

    public static IngestCoordinatorFactory defaultFactory(String localDir, StateStoreProvider stateStoreProvider, Configuration hadoopConfiguration) {
        return IngestCoordinatorFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(localDir)
                .stateStoreProvider(stateStoreProvider)
                .hadoopConfiguration(hadoopConfiguration).build();
    }

    public static IngestCoordinatorFactory defaultFactory(String localDir, StateStoreProvider stateStoreProvider) {
        return defaultFactory(localDir, stateStoreProvider, defaultHadoopConfiguration());
    }
}
