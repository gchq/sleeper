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

package sleeper.io.parquet.arrow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import sleeper.arrow.record.RecordBackedByArrow;
import sleeper.arrow.schema.SchemaBackedByArrow;

import java.io.IOException;

public class ParquetArrowWriter extends ParquetWriter<RecordBackedByArrow> {
    public ParquetArrowWriter(org.apache.hadoop.fs.Path file,
                              MessageType messageType,
                              SchemaBackedByArrow schemaBackedByArrow,
                              CompressionCodecName compressionCodecName,
                              int blockSize,
                              int pageSize) throws IOException {
        super(file, new ArrowWriteSupport(messageType, schemaBackedByArrow), compressionCodecName, blockSize, pageSize);
    }

    public ParquetArrowWriter(org.apache.hadoop.fs.Path file, MessageType messageType, SchemaBackedByArrow schemaBackedByArrow) throws IOException {
        this(file, messageType, schemaBackedByArrow, CompressionCodecName.ZSTD, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE);
    }

    public static class Builder extends ParquetWriter.Builder<RecordBackedByArrow, Builder> {
        private final MessageType messageType;
        private final SchemaBackedByArrow schema;

        public Builder(Path path, MessageType messageType, SchemaBackedByArrow schema) {
            super(path);
            this.messageType = messageType;
            this.schema = schema;
        }

        @Override
        protected WriteSupport<RecordBackedByArrow> getWriteSupport(Configuration conf) {
            return new ArrowWriteSupport(messageType, schema);
        }

        @Override
        protected ParquetArrowWriter.Builder self() {
            return this;
        }
    }
}
