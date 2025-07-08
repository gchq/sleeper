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
package sleeper.bulkimport.runner.rdd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.SleeperRow;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.MapType;
import sleeper.core.table.TableFilePaths;
import sleeper.core.util.LoggedDuration;
import sleeper.parquet.record.ParquetRecordWriterFactory;
import sleeper.sketches.Sketches;
import sleeper.sketches.store.SketchesStore;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
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
    private final SketchesStore sketchesStore;
    private ParquetWriter<SleeperRow> parquetWriter;
    private Sketches sketches;
    private String path;
    private long numRecords;
    private final String outputFilename;
    private Instant startTime;

    public SingleFileWritingIterator(
            Iterator<Row> input, InstanceProperties instanceProperties, TableProperties tableProperties,
            Configuration conf, SketchesStore sketchesStore, PartitionTree partitionTree) {
        this(input, instanceProperties, tableProperties, conf, partitionTree, sketchesStore, UUID.randomUUID().toString());
    }

    public SingleFileWritingIterator(
            Iterator<Row> input, InstanceProperties instanceProperties, TableProperties tableProperties,
            Configuration conf, PartitionTree partitionTree, SketchesStore sketchesStore, String outputFilename) {
        this.input = input;
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.schema = tableProperties.getSchema();
        this.allSchemaFields = schema.getAllFields();
        this.conf = conf;
        this.partitionTree = partitionTree;
        this.sketchesStore = sketchesStore;
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
        SleeperRow record = getRecord(row);
        parquetWriter.write(record);
        numRecords++;
        if (numRecords % 1_000_000L == 0) {
            LOGGER.info("Wrote {} records", numRecords);
        }
        sketches.update(record);
    }

    private void initialiseState(String partitionId) throws IOException {
        // Create writer
        parquetWriter = createWriter(partitionId);
        // Initialise sketches
        sketches = Sketches.from(schema);
    }

    private void closeFile() throws IOException {
        LOGGER.info("Flushing file to S3 containing {} records", numRecords);
        if (parquetWriter == null) {
            return;
        }
        parquetWriter.close();
        sketchesStore.saveFileSketches(path, schema, sketches);
        LoggedDuration duration = LoggedDuration.withFullOutput(startTime, Instant.now());
        double rate = numRecords / (double) duration.getSeconds();
        LOGGER.info("Finished writing {} records to file {} in {} (rate was {} per second)",
                numRecords, path, duration, rate);
    }

    private SleeperRow getRecord(Row row) {
        SleeperRow record = new SleeperRow();
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

    private ParquetWriter<SleeperRow> createWriter(String partitionId) throws IOException {
        numRecords = 0L;
        path = TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties)
                .constructPartitionParquetFilePath(partitionId, outputFilename);

        LOGGER.info("Creating writer for partition {} to path {}", partitionId, path);
        return ParquetRecordWriterFactory.createParquetRecordWriter(new Path(path), tableProperties, conf);
    }

    private String getPartitionId(Row row) {
        SleeperRow record = getRecord(row);
        List<String> rowKeyFieldNames = schema.getRowKeyFieldNames();
        Key key = Key.create(record.getValues(rowKeyFieldNames));
        Partition partition = partitionTree.getLeafPartition(schema, key);
        return partition.getId();
    }
}
