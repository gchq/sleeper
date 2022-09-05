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
package sleeper.io.parquet.record;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;

import java.io.IOException;

/**
 * Uses a {@link RecordWriteSupport} to write data to Parquet.
 */
public class ParquetRecordWriter extends ParquetWriter<Record> {
    public ParquetRecordWriter(org.apache.hadoop.fs.Path file,
                               MessageType messageType,
                               Schema schema,
                               CompressionCodecName compressionCodecName,
                               int blockSize,
                               int pageSize) throws IOException {
        super(file, new RecordWriteSupport(file, messageType, schema), compressionCodecName, blockSize, pageSize);
    }

    public ParquetRecordWriter(org.apache.hadoop.fs.Path file, MessageType messageType, Schema schema) throws IOException {
        this(file, messageType, schema, CompressionCodecName.ZSTD, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE);
    }

    public static class Builder extends ParquetWriter.Builder<Record, Builder> {
        private final Path path;
        private final MessageType messageType;
        private final Schema schema;

        public Builder(Path path, MessageType messageType, Schema schema) {
            super(path);
            this.path = path;
            this.messageType = messageType;
            this.schema = schema;
        }

        @Override
        protected WriteSupport<Record> getWriteSupport(Configuration conf) {
            return (WriteSupport<Record>) new RecordWriteSupport(path, messageType, schema);
        }

        @Override
        protected Builder self() {
            return (Builder) this;
        }
    }
}
