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

package sleeper.ingest.testutils;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.ingest.IngestFactory;
import sleeper.ingest.IngestResult;
import sleeper.io.parquet.record.ParquetRecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.core.properties.instance.ArrayListIngestProperty.MAX_IN_MEMORY_BATCH_SIZE;
import static sleeper.core.properties.instance.ArrayListIngestProperty.MAX_RECORDS_TO_WRITE_LOCALLY;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.DefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.instance.DefaultProperty.DEFAULT_INGEST_RECORD_BATCH_TYPE;
import static sleeper.core.properties.instance.IngestProperty.INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS;
import static sleeper.core.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.core.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.core.properties.table.TableProperty.ROW_GROUP_SIZE;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;

public class IngestRecordsTestDataHelper {

    private IngestRecordsTestDataHelper() {
    }

    public static TableProperties defaultTableProperties(Schema schema, InstanceProperties instanceProperties) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.setNumber(ROW_GROUP_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE);
        tableProperties.setNumber(PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE);
        tableProperties.set(COMPRESSION_CODEC, "zstd");
        return tableProperties;
    }

    public static InstanceProperties defaultInstanceProperties(String dataBucket) {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, dataBucket);
        instanceProperties.set(DEFAULT_INGEST_RECORD_BATCH_TYPE, "arraylist");
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        instanceProperties.setNumber(MAX_RECORDS_TO_WRITE_LOCALLY, 10L);
        instanceProperties.setNumber(MAX_IN_MEMORY_BATCH_SIZE, 1000);
        instanceProperties.setNumber(INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS, 120);
        return instanceProperties;
    }

    public static IngestFactory createIngestFactory(String localDir, StateStoreProvider stateStoreProvider, InstanceProperties instanceProperties) {
        return IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(localDir)
                .stateStoreProvider(stateStoreProvider)
                .instanceProperties(instanceProperties)
                .build();
    }

    public static Schema schemaWithRowKeys(Field... fields) {
        return Schema.builder()
                .rowKeyFields(fields)
                .valueFields(new Field("value1", new LongType()), new Field("value2", new LongType()))
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

    public static List<Record> readIngestedRecords(IngestResult result, Schema schema) {
        return result.getFileReferenceList().stream()
                .map(FileReference::getFilename)
                .flatMap(filename -> readRecordsFromParquetFileOrThrow(filename, schema).stream())
                .collect(Collectors.toList());
    }

    public static List<Record> readRecordsFromParquetFileOrThrow(String filename, Schema schema) {
        try {
            return readRecordsFromParquetFile(filename, schema);
        } catch (IOException e) {
            throw new IllegalStateException("Failed reading records", e);
        }
    }

    public static List<Record> readRecordsFromParquetFile(String filename, Schema schema) throws IOException {
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(filename), schema).build();
        List<Record> readRecords = new ArrayList<>();
        Record record = reader.read();
        while (null != record) {
            readRecords.add(cloneRecord(record, schema));
            record = reader.read();
        }
        reader.close();
        return readRecords;
    }

    private static Record cloneRecord(Record record, Schema schema) {
        Record clonedRecord = new Record();
        for (Field field : schema.getAllFields()) {
            clonedRecord.put(field.getName(), record.get(field.getName()));
        }
        return clonedRecord;
    }
}
