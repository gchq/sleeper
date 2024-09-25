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
package sleeper.bulkimport.job.runner.rdd;

import com.facebook.collections.ByteArray;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.TableFilePaths;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.MapType;
import sleeper.core.util.LoggedDuration;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;

import java.io.IOException;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SingleFileWritingIterator implements Iterator<Row> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleFileWritingIterator.class);

    private final Iterator<Row> input;
    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final Schema schema;
    private final List<Field> allSchemaFields;
    private final Configuration conf;
    private final PartitionTree partitionTree;
    private ParquetWriter<Record> parquetWriter;
    private Map<String, ItemsSketch> sketches;
    private String path;
    private long numRecords;
    private final String outputFilename;
    private Instant startTime;

    public SingleFileWritingIterator(
            Iterator<Row> input, InstanceProperties instanceProperties, TableProperties tableProperties,
            Configuration conf, PartitionTree partitionTree) {
        this(input, instanceProperties, tableProperties, conf, partitionTree, UUID.randomUUID().toString());
    }

    public SingleFileWritingIterator(
            Iterator<Row> input, InstanceProperties instanceProperties, TableProperties tableProperties,
            Configuration conf, PartitionTree partitionTree, String outputFilename) {
        this.input = input;
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.schema = tableProperties.getSchema();
        this.allSchemaFields = schema.getAllFields();
        this.conf = conf;
        this.partitionTree = partitionTree;
        this.outputFilename = outputFilename;
        LOGGER.info("Initialised SingleFileWritingIterator");
        LOGGER.info("Schema is {}", schema);
        LOGGER.info("Configuration is {}", conf);
    }

    @Override
    public boolean hasNext() {
        return input.hasNext();
    }

    @Override
    public Row next() {
        if (!hasNext()) {
            return null;
        }
        try {
            String partitionId = null;
            while (input.hasNext()) {
                Row row = input.next();
                if (null == parquetWriter) {
                    startTime = Instant.now();
                    partitionId = getPartitionId(row);
                    initialiseState(partitionId);
                }
                write(row);
            }
            closeFile();
            return RowFactory.create(partitionId, path, numRecords);
        } catch (IOException e) {
            throw new RuntimeException("Encountered error while writing files", e);
        }
    }

    private void write(Row row) throws IOException {
        Record record = getRecord(row);
        parquetWriter.write(record);
        numRecords++;
        if (numRecords % 1_000_000L == 0) {
            LOGGER.info("Wrote {} records", numRecords);
        }
        updateQuantilesSketch(record, sketches, schema.getRowKeyFields());
    }

    private void initialiseState(String partitionId) throws IOException {
        // Create writer
        parquetWriter = createWriter(partitionId);
        // Initialise sketches
        sketches = getSketches(schema.getRowKeyFields());
    }

    private void closeFile() throws IOException {
        LOGGER.info("Flushing file to S3 containing {} records", numRecords);
        if (parquetWriter == null) {
            return;
        }
        parquetWriter.close();
        new SketchesSerDeToS3(schema).saveToHadoopFS(new Path(path.replace(".parquet", ".sketches")), new Sketches(sketches), conf);
        LoggedDuration duration = LoggedDuration.withFullOutput(startTime, Instant.now());
        double rate = numRecords / (double) duration.getSeconds();
        LOGGER.info("Finished writing {} records to file {} in {} (rate was {} per second)",
                numRecords, path, duration, rate);
    }

    private Record getRecord(Row row) {
        Record record = new Record();
        int i = 0;
        for (Field field : allSchemaFields) {
            if (field.getType() instanceof ListType) {
                record.put(field.getName(), row.getList(i));
            } else if (field.getType() instanceof MapType) {
                record.put(field.getName(), row.getJavaMap(i));
            } else {
                record.put(field.getName(), row.get(i));
            }
            i++;
        }
        return record;
    }

    // TODO These methods are copies of the same ones in IngestRecordsFromIterator -
    // move to sketches module
    private Map<String, ItemsSketch> getSketches(List<Field> rowKeyFields) {
        Map<String, ItemsSketch> keyFieldToSketch = new HashMap<>();
        for (Field rowKeyField : rowKeyFields) {
            ItemsSketch<?> sketch = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
            keyFieldToSketch.put(rowKeyField.getName(), sketch);
        }
        return keyFieldToSketch;
    }

    private void updateQuantilesSketch(
            Record record, Map<String, ItemsSketch> keyFieldToSketch, List<Field> rowKeyFields) {
        for (Field rowKeyField : rowKeyFields) {
            if (rowKeyField.getType() instanceof ByteArrayType) {
                byte[] value = (byte[]) record.get(rowKeyField.getName());
                keyFieldToSketch.get(rowKeyField.getName()).update(ByteArray.wrap(value));
            } else {
                Object value = record.get(rowKeyField.getName());
                keyFieldToSketch.get(rowKeyField.getName()).update(value);
            }
        }
    }

    private ParquetWriter<Record> createWriter(String partitionId) throws IOException {
        numRecords = 0L;
        path = TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties)
                .constructPartitionParquetFilePath(partitionId, outputFilename);

        LOGGER.info("Creating writer for partition {} to path {}", partitionId, path);
        return ParquetRecordWriterFactory.createParquetRecordWriter(new Path(path), tableProperties, conf);
    }

    private String getPartitionId(Row row) {
        Record record = getRecord(row);
        List<String> rowKeyFieldNames = schema.getRowKeyFieldNames();
        Key key = Key.create(record.getValues(rowKeyFieldNames));
        Partition partition = partitionTree.getLeafPartition(schema, key);
        return partition.getId();
    }
}
