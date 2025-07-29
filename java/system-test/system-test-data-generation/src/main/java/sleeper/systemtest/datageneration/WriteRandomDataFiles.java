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
package sleeper.systemtest.datageneration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.parquet.row.ParquetRowWriterFactory;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.systemtest.configuration.SystemTestDataGenerationJob;
import sleeper.systemtest.configuration.SystemTestPropertyValues;

import java.io.IOException;
import java.util.Iterator;

import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_BUCKET_NAME;

public class WriteRandomDataFiles {
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteRandomDataFiles.class);

    private WriteRandomDataFiles() {
    }

    public static String writeToS3GetDirectory(
            SystemTestPropertyValues systemTestProperties, TableProperties tableProperties, Configuration hadoopConf, SystemTestDataGenerationJob job) throws IOException {

        String dir = systemTestProperties.get(SYSTEM_TEST_BUCKET_NAME) + "/ingest/" + job.getJobId();

        writeToPath(dir, "s3a://", tableProperties,
                WriteRandomData.createRowIterator(job, tableProperties),
                hadoopConf);
        return dir;
    }

    public static void writeFilesToDirectory(
            String directory, InstanceProperties instanceProperties,
            TableProperties tableProperties, Iterator<Row> rowIterator) throws IOException {
        Configuration conf = HadoopConfigurationProvider.getConfigurationForECS(instanceProperties);
        writeToPath(directory, "file:///", tableProperties, rowIterator, conf);
    }

    private static void writeToPath(
            String dir, String filePathPrefix, TableProperties tableProperties, Iterator<Row> rowIterator,
            Configuration conf) throws IOException {
        int fileNumber = 0;
        if (!dir.endsWith("/")) {
            dir = dir + "/";
        }
        String filename = dir + fileNumber + ".parquet";
        String path = filePathPrefix + filename;
        ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(new Path(path), tableProperties, conf);
        long count = 0L;
        LOGGER.info("Created writer to path {}", path);
        while (rowIterator.hasNext()) {
            writer.write(rowIterator.next());
            count++;
            if (0 == count % 1_000_000L) {
                LOGGER.info("Wrote {} rows", count);
                if (0 == count % 100_000_000L) {
                    writer.close();
                    LOGGER.info("Closed writer to path {}", path);
                    fileNumber++;
                    filename = dir + fileNumber + ".parquet";
                    path = filePathPrefix + filename;
                    writer = ParquetRowWriterFactory.createParquetRowWriter(new Path(path), tableProperties, conf);
                }
            }
        }
        LOGGER.info("Closed writer to path {}", path);
        writer.close();
        LOGGER.info("Wrote {} rows", count);
    }
}
