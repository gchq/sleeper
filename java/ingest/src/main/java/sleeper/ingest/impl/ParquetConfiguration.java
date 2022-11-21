/*
 * Copyright 2022 Crown Copyright
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
package sleeper.ingest.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.io.parquet.record.ParquetRecordWriter;
import sleeper.io.parquet.record.SchemaConverter;

import java.io.IOException;
import java.util.Objects;

import static sleeper.configuration.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.configuration.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.configuration.properties.table.TableProperty.ROW_GROUP_SIZE;

public class ParquetConfiguration {

    private final Schema sleeperSchema;
    private final String parquetCompressionCodec;
    private final long parquetRowGroupSize;
    private final int parquetPageSize;
    private final Configuration hadoopConfiguration;

    private ParquetConfiguration(Builder builder) {
        sleeperSchema = Objects.requireNonNull(builder.sleeperSchema, "sleeperSchema must not be null");
        parquetCompressionCodec = Objects.requireNonNull(builder.parquetCompressionCodec, "parquetCompressionCodec must not be null");
        parquetRowGroupSize = builder.parquetRowGroupSize;
        parquetPageSize = builder.parquetPageSize;
        if (parquetRowGroupSize < 1) {
            throw new IllegalArgumentException("parquetRowGroupSize must be positive");
        }
        if (parquetPageSize < 1) {
            throw new IllegalArgumentException("parquetPageSize must be positive");
        }
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

    public Schema getSleeperSchema() {
        return sleeperSchema;
    }

    public Configuration getHadoopConfiguration() {
        return hadoopConfiguration;
    }

    /**
     * Create a {@link ParquetWriter} to write {@link Record} objects using the parameter values supplied during
     * construction. It is the responsibility of the caller to close the writer after use.
     *
     * @param outputFile The name of the Parquet file to write to
     * @return The {@link ParquetWriter} object
     * @throws IOException Thrown when the writer cannot be created
     */
    public ParquetWriter<Record> createParquetWriter(String outputFile) throws IOException {
        ParquetRecordWriter.Builder builder = new ParquetRecordWriter.Builder(new Path(outputFile),
                SchemaConverter.getSchema(sleeperSchema), sleeperSchema)
                .withCompressionCodec(CompressionCodecName.fromConf(parquetCompressionCodec))
                .withRowGroupSize(parquetRowGroupSize)
                .withPageSize(parquetPageSize)
                .withConf(hadoopConfiguration);
        return builder.build();
    }

    public static final class Builder {
        private Schema sleeperSchema;
        private String parquetCompressionCodec;
        private long parquetRowGroupSize = ParquetWriter.DEFAULT_BLOCK_SIZE;
        private int parquetPageSize = ParquetWriter.DEFAULT_PAGE_SIZE;
        private Configuration hadoopConfiguration;

        private Builder() {
        }

        public Builder tableProperties(TableProperties tableProperties) {
            return sleeperSchema(tableProperties.getSchema())
                    .parquetCompressionCodec(tableProperties.get(COMPRESSION_CODEC))
                    .parquetRowGroupSize(tableProperties.getLong(ROW_GROUP_SIZE))
                    .parquetPageSize(tableProperties.getInt(PAGE_SIZE));
        }

        public Builder sleeperSchema(Schema sleeperSchema) {
            this.sleeperSchema = sleeperSchema;
            return this;
        }

        public Builder parquetCompressionCodec(String parquetCompressionCodec) {
            this.parquetCompressionCodec = parquetCompressionCodec;
            return this;
        }

        public Builder parquetRowGroupSize(long parquetRowGroupSize) {
            this.parquetRowGroupSize = parquetRowGroupSize;
            return this;
        }

        public Builder parquetPageSize(int parquetPageSize) {
            this.parquetPageSize = parquetPageSize;
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
