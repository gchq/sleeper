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

import sleeper.bulkimport.runner.common.SparkRowMapper;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableFilePaths;
import sleeper.core.util.LoggedDuration;
import sleeper.parquet.row.ParquetRowWriterFactory;
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
    private final SparkRowMapper rowMapper;
    private final Configuration conf;
    private final PartitionTree partitionTree;
    private final SketchesStore sketchesStore;
    private ParquetWriter<sleeper.core.row.Row> parquetWriter;
    private Sketches sketches;
    private String path;
    private long numRows;
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
        this.rowMapper = new SparkRowMapper(schema);
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
            return RowFactory.create(partitionId, path, numRows);
        } catch (IOException e) {
            throw new RuntimeException("Encountered error while writing files", e);
        }
    }

    private void write(Row row) throws IOException {
        sleeper.core.row.Row writeRow = rowMapper.toSleeperRow(row);
        parquetWriter.write(writeRow);
        numRows++;
        if (numRows % 1_000_000L == 0) {
            LOGGER.info("Wrote {} rows", numRows);
        }
        sketches.update(writeRow);
    }

    private void initialiseState(String partitionId) throws IOException {
        // Create writer
        parquetWriter = createWriter(partitionId);
        // Initialise sketches
        sketches = Sketches.from(schema);
    }

    private void closeFile() throws IOException {
        LOGGER.info("Flushing file to S3 containing {} rows", numRows);
        if (parquetWriter == null) {
            return;
        }
        parquetWriter.close();
        sketchesStore.saveFileSketches(path, schema, sketches);
        LoggedDuration duration = LoggedDuration.withFullOutput(startTime, Instant.now());
        double rate = numRows / (double) duration.getSeconds();
        LOGGER.info("Finished writing {} rows to file {} in {} (rate was {} per second)",
                numRows, path, duration, rate);
    }

    private ParquetWriter<sleeper.core.row.Row> createWriter(String partitionId) throws IOException {
        numRows = 0L;
        path = TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties)
                .constructPartitionParquetFilePath(partitionId, outputFilename);

        LOGGER.info("Creating writer for partition {} to path {}", partitionId, path);
        return ParquetRowWriterFactory.createParquetRowWriter(new Path(path), tableProperties, conf);
    }

    private String getPartitionId(Row row) {
        sleeper.core.row.Row keyRow = rowMapper.toSleeperRow(row);
        List<String> rowKeyFieldNames = schema.getRowKeyFieldNames();
        Key key = Key.create(keyRow.getValues(rowKeyFieldNames));
        Partition partition = partitionTree.getLeafPartition(schema, key);
        return partition.getId();
    }
}
