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

package sleeper.systemtest.drivers.sourcedata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesDriver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;

import static sleeper.sketches.s3.SketchesSerDeToS3.sketchesPathForDataFile;

public class AwsIngestSourceFilesDriver implements IngestSourceFilesDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsIngestSourceFilesDriver.class);
    private final Configuration configuration;

    public AwsIngestSourceFilesDriver(Configuration configuration) {
        this.configuration = configuration;
    }

    public void writeFile(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            String path, boolean writeSketches, Iterator<Record> records) {
        Schema schema = tableProperties.getSchema();
        Sketches sketches = Sketches.from(schema);
        LOGGER.info("Writing to {}", path);
        try (ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(
                new Path(path), tableProperties, configuration)) {
            for (Record record : (Iterable<Record>) () -> records) {
                sketches.update(schema, record);
                writer.write(record);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (writeSketches) {
            LOGGER.info("Writing sketches");
            try {
                new SketchesSerDeToS3(schema).saveToHadoopFS(sketchesPathForDataFile(path), sketches, configuration);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
