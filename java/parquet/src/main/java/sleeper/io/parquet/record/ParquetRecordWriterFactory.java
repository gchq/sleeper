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

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static sleeper.configuration.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.configuration.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS;
import static sleeper.configuration.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS;
import static sleeper.configuration.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_VALUE_FIELDS;
import static sleeper.configuration.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.configuration.properties.table.TableProperty.ROW_GROUP_SIZE;

public class ParquetRecordWriterFactory {

    private ParquetRecordWriterFactory() {
    }

    public static ParquetWriter<Record> createParquetRecordWriter(Path path, Schema schema) throws IOException {
        return new Builder(path, SchemaConverter.getSchema(schema), schema).build();
    }

    public static ParquetWriter<Record> createParquetRecordWriter(Path path, TableProperties tableProperties, Configuration conf) throws IOException {
        return createParquetRecordWriter(path,
            tableProperties.getSchema(),
            tableProperties.get(COMPRESSION_CODEC),
            tableProperties.getLong(ROW_GROUP_SIZE),
            tableProperties.getInt(PAGE_SIZE),
            tableProperties.getBoolean(DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS),
            tableProperties.getBoolean(DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS),
            tableProperties.getBoolean(DICTIONARY_ENCODING_FOR_VALUE_FIELDS),
            conf);
    }

    public static ParquetWriter<Record> createParquetRecordWriter(Path path, Schema schema, Configuration conf) throws IOException {
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.setSchema(schema);
        return createParquetRecordWriter(path, tableProperties, conf);
    }

    public static ParquetWriter<Record> createParquetRecordWriter(Path path,
            Schema schema,
            String compressionCodec,
            long rowGroupSize,
            int pageSize,
            boolean dictionaryEncodingForRowKeyFields,
            boolean dictionaryEncodingForSortKeyFields,
            boolean dictionaryEncodingForValueFields,
            Configuration conf) throws IOException {
        Builder builder = new Builder(path, SchemaConverter.getSchema(schema), schema)
            .withCompressionCodec(CompressionCodecName.fromConf(compressionCodec.toUpperCase(Locale.ROOT)))
            .withRowGroupSize(rowGroupSize)
            .withPageSize(pageSize)
            .withConf(conf);

        setDictionaryEncoding(builder, schema.getRowKeyFieldNames(), dictionaryEncodingForRowKeyFields);
        setDictionaryEncoding(builder, schema.getSortKeyFieldNames(), dictionaryEncodingForSortKeyFields);
        setDictionaryEncoding(builder, schema.getValueFieldNames(), dictionaryEncodingForValueFields);

        return builder.build();
    }

    private static void setDictionaryEncoding(Builder builder, List<String> fieldNames, boolean dictionaryEncodingEnabled) {
        for (String fieldName : fieldNames) {
            builder = builder.withDictionaryEncoding(fieldName, dictionaryEncodingEnabled);
        }
    }

    public static class Builder extends ParquetWriter.Builder<Record, Builder> {
        private final MessageType messageType;
        private final Schema schema;

        public Builder(Path path, MessageType messageType, Schema schema) {
            super(path);
            this.messageType = messageType;
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
    }
}
