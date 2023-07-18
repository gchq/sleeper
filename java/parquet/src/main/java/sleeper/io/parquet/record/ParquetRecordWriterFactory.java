/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.io.parquet.record;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static sleeper.configuration.properties.table.TableProperty.COLUMN_INDEX_TRUNCATE_LENGTH;
import static sleeper.configuration.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.configuration.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS;
import static sleeper.configuration.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS;
import static sleeper.configuration.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_VALUE_FIELDS;
import static sleeper.configuration.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.configuration.properties.table.TableProperty.ROW_GROUP_SIZE;
import static sleeper.configuration.properties.table.TableProperty.STATISTICS_TRUNCATE_LENGTH;

public class ParquetRecordWriterFactory {

    private ParquetRecordWriterFactory() {
    }

    public static ParquetWriter<Record> createParquetRecordWriter(Path path, Schema schema) throws IOException {
        return createParquetRecordWriter(path, schema, new Configuration());
    }

    public static ParquetWriter<Record> createParquetRecordWriter(Path path, Schema schema, Configuration conf) throws IOException {
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.setSchema(schema);
        return createParquetRecordWriter(path, tableProperties, conf);
    }

    public static ParquetWriter<Record> createParquetRecordWriter(Path path, TableProperties tableProperties, Configuration conf) throws IOException {
        return parquetRecordWriterBuilder(path, tableProperties)
                .withConf(conf).build();
    }

    public static Builder parquetRecordWriterBuilder(Path path, TableProperties tableProperties) {
        return new Builder(path, tableProperties.getSchema())
                .withCompressionCodec(tableProperties.get(COMPRESSION_CODEC))
                .withRowGroupSize(tableProperties.getLong(ROW_GROUP_SIZE))
                .withPageSize(tableProperties.getInt(PAGE_SIZE))
                .withDictionaryEncodingForRowKeyFields(tableProperties.getBoolean(DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS))
                .withDictionaryEncodingForSortKeyFields(tableProperties.getBoolean(DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS))
                .withDictionaryEncodingForValueFields(tableProperties.getBoolean(DICTIONARY_ENCODING_FOR_VALUE_FIELDS))
                .withColumnIndexTruncateLength(tableProperties.getInt(COLUMN_INDEX_TRUNCATE_LENGTH))
                .withStatisticsTruncateLength(tableProperties.getInt(STATISTICS_TRUNCATE_LENGTH));
    }

    public static class Builder extends ParquetWriter.Builder<Record, Builder> {
        private final MessageType messageType;
        private final Schema schema;

        private Builder(Path path, Schema schema) {
            super(path);
            this.messageType = SchemaConverter.getSchema(schema);
            this.schema = schema;
        }

        @Override
        protected WriteSupport<Record> getWriteSupport(Configuration conf) {
            return new RecordWriteSupport(messageType, schema);
        }

        @Override
        protected Builder self() {
            return this;
        }

        public Builder withCompressionCodec(String compressionCodec) {
            return withCompressionCodec(CompressionCodecName.fromConf(compressionCodec.toUpperCase(Locale.ROOT)));
        }

        public Builder withDictionaryEncodingForRowKeyFields(boolean dictionaryEncodingForRowKeyFields) {
            setDictionaryEncoding(this, schema.getRowKeyFieldNames(), dictionaryEncodingForRowKeyFields);
            return this;
        }

        public Builder withDictionaryEncodingForSortKeyFields(boolean dictionaryEncodingForSortKeyFields) {
            setDictionaryEncoding(this, schema.getSortKeyFieldNames(), dictionaryEncodingForSortKeyFields);
            return this;
        }

        public Builder withDictionaryEncodingForValueFields(boolean dictionaryEncodingForValueFields) {
            setDictionaryEncoding(this, schema.getValueFieldNames(), dictionaryEncodingForValueFields);
            return this;
        }
    }

    private static void setDictionaryEncoding(Builder builder, List<String> fieldNames, boolean dictionaryEncodingEnabled) {
        for (String fieldName : fieldNames) {
            builder = builder.withDictionaryEncoding(fieldName, dictionaryEncodingEnabled);
        }
    }
}
