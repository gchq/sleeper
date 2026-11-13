/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.parquet.row;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import sleeper.core.row.Row;

import java.util.Map;

/**
 * Reads Sleeper rows from a Parquet file without knowing its schema in advance. Reads every field in the schema held
 * in the file. See {@link GenericRowConverter} for how each Parquet type is read.
 */
class GenericRowReadSupport extends ReadSupport<Row> {

    @Override
    public ReadContext init(InitContext context) {
        return new ReadContext(context.getFileSchema());
    }

    @Override
    public RecordMaterializer<Row> prepareForRead(
            Configuration configuration,
            Map<String, String> keyValueMetaData,
            MessageType fileSchema,
            ReadContext readContext) {
        return new GenericRowMaterializer(new GenericRowConverter(readContext.getRequestedSchema()));
    }

    private static class GenericRowMaterializer extends RecordMaterializer<Row> {
        private final GenericRowConverter converter;

        GenericRowMaterializer(GenericRowConverter converter) {
            this.converter = converter;
        }

        @Override
        public Row getCurrentRecord() {
            return converter.getRow();
        }

        @Override
        public GroupConverter getRootConverter() {
            return converter;
        }
    }
}
