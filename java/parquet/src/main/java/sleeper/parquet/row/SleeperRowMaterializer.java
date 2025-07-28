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
package sleeper.parquet.row;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;

import sleeper.core.row.Row;
import sleeper.core.schema.Schema;

class SleeperRowMaterializer extends RecordMaterializer<Row> {
    private final RowConverter rowConverter;

    SleeperRowMaterializer(Schema schema) {
        this.rowConverter = new RowConverter(schema);
    }

    @Override
    public Row getCurrentRecord() {
        return rowConverter.getRow();
    }

    @Override
    public GroupConverter getRootConverter() {
        return rowConverter;
    }
}
