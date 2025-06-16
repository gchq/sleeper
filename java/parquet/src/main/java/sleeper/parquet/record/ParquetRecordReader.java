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
package sleeper.parquet.record;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;

import sleeper.core.record.Record;
import sleeper.core.schema.Schema;

import java.io.IOException;

/**
 * Reads Parquet files. Uses {@link RecordReadSupport}.
 */
public class ParquetRecordReader extends ParquetReader<Record> {

    public ParquetRecordReader(String file, Schema schema) throws IOException {
        super(new Path(file), new RecordReadSupport(schema));
    }

    public static class Builder extends ParquetReader.Builder<Record> {
        private final Schema schema;

        public Builder(String path, Schema schema) {
            super(new Path(path));
            this.schema = schema;
            useColumnIndexFilter(false);
        }

        @Override
        protected ReadSupport<Record> getReadSupport() {
            return new RecordReadSupport(schema);
        }
    }
}
