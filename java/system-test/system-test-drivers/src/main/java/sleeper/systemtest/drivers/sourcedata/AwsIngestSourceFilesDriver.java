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

package sleeper.systemtest.drivers.sourcedata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.parquet.row.ParquetRowWriterFactory;
import sleeper.sketches.Sketches;
import sleeper.sketches.store.S3SketchesStore;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesDriver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;

public class AwsIngestSourceFilesDriver implements IngestSourceFilesDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsIngestSourceFilesDriver.class);
    private final SystemTestClients clients;

    public AwsIngestSourceFilesDriver(SystemTestClients clients) {
        this.clients = clients;
    }

    public void writeFile(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            String path, boolean writeSketches, Iterator<Row> rows) {
        Schema schema = tableProperties.getSchema();
        Configuration conf = clients.createHadoopConf(instanceProperties, tableProperties);
        Sketches sketches = Sketches.from(schema);
        LOGGER.info("Writing to {}", path);
        try (ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(
                new Path(path), tableProperties, conf)) {
            for (Row row : (Iterable<Row>) () -> rows) {
                sketches.update(row);
                writer.write(row);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (writeSketches) {
            LOGGER.info("Writing sketches");
            S3SketchesStore.createWriteOnly(clients.getS3TransferManager())
                    .saveFileSketches(path, schema, sketches);
        }
    }
}
