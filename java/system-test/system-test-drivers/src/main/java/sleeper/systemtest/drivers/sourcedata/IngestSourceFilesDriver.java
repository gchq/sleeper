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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;

import static sleeper.sketches.s3.SketchesSerDeToS3.sketchesPathForDataFile;

public class IngestSourceFilesDriver {

    private final IngestSourceFilesContext context;

    public IngestSourceFilesDriver(IngestSourceFilesContext context) {
        this.context = context;
    }

    public void writeFile(InstanceProperties instanceProperties, TableProperties tableProperties,
                          String file, boolean writeSketches, Iterator<Record> records) {
        Schema schema = tableProperties.getSchema();
        Configuration conf = HadoopConfigurationProvider.getConfigurationForClient(instanceProperties, tableProperties);
        Sketches sketches = Sketches.from(schema);
        String path = "s3a://" + context.getSourceBucketName() + "/" + file;
        try (ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(
                new Path(path), tableProperties, conf)) {
            for (Record record : (Iterable<Record>) () -> records) {
                sketches.update(schema, record);
                writer.write(record);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (writeSketches) {
            try {
                new SketchesSerDeToS3(schema).saveToHadoopFS(sketchesPathForDataFile(path), sketches, conf);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        context.wroteFile(file, path);
    }
}
