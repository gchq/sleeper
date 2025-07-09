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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.schema.MessageType;

import sleeper.core.row.Row;
import sleeper.core.schema.Schema;

import java.util.Map;

public class RecordReadSupport extends ReadSupport<Row> {
    private final Schema schema;

    public RecordReadSupport(Schema schema) {
        this.schema = schema;
    }

    @Override
    public org.apache.parquet.io.api.RecordMaterializer<Row> prepareForRead(
            Configuration configuration,
            Map<String, String> keyValueMetaData,
            MessageType fileSchema,
            ReadContext readContext) {
        return new SleeperRowMaterializer(schema);
    }

    @Override
    public ReadContext init(InitContext context) {
        return new ReadContext(SchemaConverter.getSchema(schema));
    }
}
