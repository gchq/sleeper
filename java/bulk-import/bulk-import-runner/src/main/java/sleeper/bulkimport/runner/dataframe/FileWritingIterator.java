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
package sleeper.bulkimport.runner.dataframe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
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
import java.util.function.Supplier;

public class FileWritingIterator implements Iterator<Row> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileWritingIterator.class);

    private final Iterator<Row> input;
    private final Schema schema;
    private final List<Field> allSchemaFields;
    private final Configuration conf;
    private final SketchesStore sketchesStore;
    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final Supplier<String> outputFilenameSupplier;
    private String currentPartitionId;
    private ParquetWriter<sleeper.core.row.Row> parquetWriter;
    private Sketches sketches;
    private String path;
    private long numRows;
    private boolean hasMore = false;
    private Instant startTime = null;

    public FileWritingIterator(
            Iterator<Row> input, InstanceProperties instanceProperties, TableProperties tableProperties,
            Configuration conf, SketchesStore sketchesStore) {
        this(input, instanceProperties, tableProperties, conf, sketchesStore, () -> UUID.randomUUID().toString());
    }

    public FileWritingIterator(
            Iterator<Row> input, InstanceProperties instanceProperties, TableProperties tableProperties,
            Configuration conf, SketchesStore sketchesStore, Supplier<String> outputFilenameSupplier) {
        this.input = input;
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.schema = tableProperties.getSchema();
        this.allSchemaFields = schema.getAllFields();
        this.conf = conf;
        this.sketchesStore = sketchesStore;
        this.outputFilenameSupplier = outputFilenameSupplier;
        LOGGER.info("Initialised FileWritingIterator");
        LOGGER.info("Schema is {}", schema);
        LOGGER.info("Configuration is {}", conf);
    }

    @Override
    public boolean hasNext() {
        return input.hasNext() || hasMore;
    }

    @Override
    public Row next() {
        if (!hasNext()) {
            return null;
        }
        try {
            while (input.hasNext()) {
                Row row = input.next();
                // Get Partition Id for this row
                String partitionId = getPartitionId(row);

                if (!partitionId.equals(currentPartitionId)) {
                    if (currentPartitionId != null) {
                        // Write file and sketches
                        writeFiles();
                        Row fileReference = RowFactory.create(currentPartitionId, path, numRows);
                        initialiseState(partitionId);
                        write(row);
                        // Set flag in case this is the last row in the iterator
                        hasMore = true;
                        return fileReference;
                    } else {
                        initialiseState(partitionId);
                    }
                }
                write(row);
            }

            // Flush final file
            writeFiles();
            hasMore = false;
            return RowFactory.create(currentPartitionId, path, numRows);
        } catch (IOException e) {
            throw new RuntimeException("Encountered error while writing files", e);
        }
    }

    private void write(Row row) throws IOException {
        if (null == startTime) {
            startTime = Instant.now();
        }
        // Append to current writer
        sleeper.core.row.Row writeRow = getRow(row);
        parquetWriter.write(writeRow);
        numRows++;
        if (numRows % 1_000_000L == 0) {
            LOGGER.info("Wrote {} rows", numRows);
        }
        sketches.update(writeRow);
    }

    private void initialiseState(String partitionId) throws IOException {
        currentPartitionId = partitionId;
        // Create writer;
        parquetWriter = createWriter(partitionId);
        // Initialise sketches
        sketches = Sketches.from(schema);
    }

    private void writeFiles() throws IOException {
        LOGGER.info("Flushing files to S3 containing {} rows", numRows);
        if (parquetWriter == null) {
            return;
        }
        parquetWriter.close();
        sketchesStore.saveFileSketches(path, schema, sketches);
        LoggedDuration duration = LoggedDuration.withFullOutput(startTime, Instant.now());
        double rate = numRows / (double) duration.getSeconds();
        LOGGER.info("Overall written {} rows in {} (rate was {} per second)",
                numRows, duration, rate);
    }

    private sleeper.core.row.Row getRow(Row row) {
        sleeper.core.row.Row outRow = new sleeper.core.row.Row();
        int i = 0;
        for (Field field : allSchemaFields) {
            if (field.getType() instanceof ListType) {
                outRow.put(field.getName(), row.getList(i));
            } else if (field.getType() instanceof MapType) {
                outRow.put(field.getName(), row.getJavaMap(i));
            } else {
                outRow.put(field.getName(), row.get(i));
            }
            i++;
        }
        return outRow;
    }

    private ParquetWriter<sleeper.core.row.Row> createWriter(String partitionId) throws IOException {
        numRows = 0L;
        path = TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties)
                .constructPartitionParquetFilePath(partitionId, outputFilenameSupplier.get());

        LOGGER.info("Creating writer for partition {} to path {}", partitionId, path);

        return ParquetRecordWriterFactory.createParquetRecordWriter(new Path(path), tableProperties, conf);
    }

    private String getPartitionId(Row row) {
        return row.getString(row.length() - 1);
    }
}
