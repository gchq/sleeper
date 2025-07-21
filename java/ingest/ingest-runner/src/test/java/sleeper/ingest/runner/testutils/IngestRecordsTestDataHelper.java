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

package sleeper.ingest.runner.testutils;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.core.IngestResult;
import sleeper.ingest.runner.IngestFactory;
import sleeper.parquet.record.ParquetRecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.core.properties.instance.ArrayListIngestProperty.MAX_IN_MEMORY_BATCH_SIZE;
import static sleeper.core.properties.instance.ArrayListIngestProperty.MAX_RECORDS_TO_WRITE_LOCALLY;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.IngestProperty.INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_RECORD_BATCH_TYPE;
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

    public static List<Row> getSingleRow() {
        Row row1 = new Row();
        row1.put("key", 1L);
        row1.put("value1", 2L);
        row1.put("value2", 3L);
        return Collections.singletonList(row1);
    }

    public static List<Row> getRows() {
        List<Row> rows = new ArrayList<>();
        Row row1 = new Row();
        row1.put("key", 1L);
        row1.put("value1", 2L);
        row1.put("value2", 3L);
        Row row2 = new Row();
        row2.put("key", 3L);
        row2.put("value1", 4L);
        row2.put("value2", 6L);
        rows.add(row1);
        rows.add(row2);
        return rows;
    }

    public static List<Row> getLotsOfRows() {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            Row row1 = new Row();
            row1.put("key", 1L - i);
            row1.put("value1", 2L * i);
            row1.put("value2", 3L * i);
            Row row2 = new Row();
            row2.put("key", 2L + i);
            row2.put("value1", 4L * i);
            row2.put("value2", 6L * i);
            rows.add(row1);
            rows.add(row2);
        }
        return rows;
    }

    public static List<Row> getRowsInFirstPartitionOnly() {
        List<Row> rows = new ArrayList<>();
        Row row1 = new Row();
        row1.put("key", 1L);
        row1.put("value1", 2L);
        row1.put("value2", 3L);
        Row row2 = new Row();
        row2.put("key", 0L);
        row2.put("value1", 4L);
        row2.put("value2", 6L);
        rows.add(row1);
        rows.add(row2);
        return rows;
    }

    public static List<Row> getRowsByteArrayKey() {
        List<Row> rows = new ArrayList<>();
        Row row1 = new Row();
        row1.put("key", new byte[]{1, 1});
        row1.put("value1", 2L);
        row1.put("value2", 3L);
        Row row2 = new Row();
        row2.put("key", new byte[]{2, 2});
        row2.put("value1", 2L);
        row2.put("value2", 3L);
        Row row3 = new Row();
        row3.put("key", new byte[]{64, 65});
        row3.put("value1", 4L);
        row3.put("value2", 6L);
        rows.add(row1);
        rows.add(row2);
        rows.add(row3);
        return rows;
    }

    public static List<Row> getRows2DimByteArrayKey() {
        List<Row> rows = new ArrayList<>();
        Row row1 = new Row();
        row1.put("key1", new byte[]{1, 1});
        row1.put("key2", new byte[]{2, 3});
        row1.put("value1", 2L);
        row1.put("value2", 3L);
        Row row2 = new Row();
        row2.put("key1", new byte[]{11, 2});
        row2.put("key2", new byte[]{2, 2});
        row2.put("value1", 2L);
        row2.put("value2", 3L);
        Row row3 = new Row();
        row3.put("key1", new byte[]{64, 65});
        row3.put("key2", new byte[]{67, 68});
        row3.put("value1", 4L);
        row3.put("value2", 6L);
        Row row4 = new Row();
        row4.put("key1", new byte[]{5});
        row4.put("key2", new byte[]{99});
        row4.put("value1", 2L);
        row4.put("value2", 3L);
        rows.add(row1);
        rows.add(row2);
        rows.add(row3);
        rows.add(row3); // Add twice so that one file has more entries so we can tell them apart
        rows.add(row4);
        return rows;
    }

    public static List<Row> getUnsortedRows() {
        List<Row> rows = new ArrayList<>();
        for (int i = 10; i > 0; i--) {
            Row row1 = new Row();
            row1.put("key", (long) i);
            row1.put("value1", 2L);
            row1.put("value2", 3L);
            rows.add(row1);
            Row row2 = new Row();
            row2.put("key", 5L);
            row2.put("value1", 4L);
            row2.put("value2", 6L);
            rows.add(row2);
        }
        return rows;
    }

    public static List<Row> getRowsForAggregationIteratorTest() {
        List<Row> rows = new ArrayList<>();
        Row row1 = new Row();
        row1.put("key", new byte[]{1, 1});
        row1.put("sort", 2L);
        row1.put("value", 1L);
        Row row2 = new Row();
        row2.put("key", new byte[]{11, 2});
        row2.put("sort", 1L);
        row2.put("value", 1L);
        Row row3 = new Row();
        row3.put("key", new byte[]{1, 1});
        row3.put("sort", 2L);
        row3.put("value", 6L);
        Row row4 = new Row();
        row4.put("key", new byte[]{11, 2});
        row4.put("sort", 1L);
        row4.put("value", 3L);
        rows.add(row1);
        rows.add(row2);
        rows.add(row3);
        rows.add(row4);
        return rows;
    }

    public static List<Row> getRowsOscillatingBetween2Partitions() {
        List<Row> rows = new ArrayList<>();
        Row row1 = new Row();
        row1.put("key1", 0);
        row1.put("key2", 1L);
        row1.put("value1", 2L);
        row1.put("value2", 1L);
        Row row2 = new Row();
        row2.put("key1", 0);
        row2.put("key2", 20L);
        row2.put("value1", 200L);
        row2.put("value2", 100L);
        Row row3 = new Row();
        row3.put("key1", 100);
        row3.put("key2", 1L);
        row3.put("value1", 20000L);
        row3.put("value2", 10000L);
        Row row4 = new Row();
        row4.put("key1", 100);
        row4.put("key2", 50L);
        row4.put("value1", 2000000L);
        row4.put("value2", 1000000L);
        rows.add(row1);
        rows.add(row2);
        rows.add(row3);
        rows.add(row4);
        return rows;
    }

    public static List<Row> readIngestedRecords(IngestResult result, Schema schema) {
        return result.getFileReferenceList().stream()
                .map(FileReference::getFilename)
                .flatMap(filename -> readRecordsFromParquetFileOrThrow(filename, schema).stream())
                .collect(Collectors.toList());
    }

    public static List<Row> readRecordsFromParquetFileOrThrow(String filename, Schema schema) {
        try {
            return readRecordsFromParquetFile(filename, schema);
        } catch (IOException e) {
            throw new IllegalStateException("Failed reading records", e);
        }
    }

    public static List<Row> readRecordsFromParquetFile(String filename, Schema schema) throws IOException {
        ParquetReader<Row> reader = new ParquetRecordReader.Builder(new Path(filename), schema).build();
        List<Row> readRows = new ArrayList<>();
        Row row = reader.read();
        while (null != row) {
            readRows.add(cloneRow(row, schema));
            row = reader.read();
        }
        reader.close();
        return readRows;
    }

    private static Row cloneRow(Row row, Schema schema) {
        Row clonedRow = new Row();
        for (Field field : schema.getAllFields()) {
            clonedRow.put(field.getName(), row.get(field.getName()));
        }
        return clonedRow;
    }
}
