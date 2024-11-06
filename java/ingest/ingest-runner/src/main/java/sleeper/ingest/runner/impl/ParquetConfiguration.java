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
package sleeper.ingest.runner.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;

import java.io.IOException;
import java.util.Objects;

public class ParquetConfiguration {
    private final TableProperties tableProperties;
    private final Configuration hadoopConfiguration;

    private ParquetConfiguration(Builder builder) {
        tableProperties = Objects.requireNonNull(builder.tableProperties, "tableProperties must not be null");
        hadoopConfiguration = Objects.requireNonNull(builder.hadoopConfiguration, "hadoopConfiguration must not be null");
    }

    public static ParquetConfiguration from(TableProperties tableProperties, Configuration hadoopConfiguration) {
        return builderWith(tableProperties).hadoopConfiguration(hadoopConfiguration).build();
    }

    public static Builder builderWith(TableProperties tableProperties) {
        return builder().tableProperties(tableProperties);
    }

    public static Builder builder() {
        return new Builder();
    }

    public TableProperties getTableProperties() {
        return tableProperties;
    }

    public Configuration getHadoopConfiguration() {
        return hadoopConfiguration;
    }

    /**
     * Create a writer to write records to a Parquet file. It is the responsibility of the caller to close the writer
     * after use.
     *
     * @param  outputFile  The name of the Parquet file to write to
     * @return             The {@link ParquetWriter} object
     * @throws IOException Thrown when the writer cannot be created
     */
    public ParquetWriter<Record> createParquetWriter(String outputFile) throws IOException {
        return ParquetRecordWriterFactory.createParquetRecordWriter(new Path(outputFile), tableProperties, hadoopConfiguration);
    }

    public static final class Builder {
        private Configuration hadoopConfiguration;
        private TableProperties tableProperties;

        private Builder() {
        }

        public Builder tableProperties(TableProperties tableProperties) {
            this.tableProperties = tableProperties;
            return this;
        }

        public Builder hadoopConfiguration(Configuration hadoopConfiguration) {
            this.hadoopConfiguration = hadoopConfiguration;
            return this;
        }

        public ParquetConfiguration build() {
            return new ParquetConfiguration(this);
        }
    }
}
