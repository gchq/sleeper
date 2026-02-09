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

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;

import sleeper.core.row.Row;
import sleeper.core.schema.Schema;

/**
 * Factory to create parquet reader for sleeper rows. Uses {@link RowReadSupport}.
 */
public class ParquetRowReaderFactory {

    private ParquetRowReaderFactory() {
    }

    public static ParquetReader.Builder<Row> parquetRowReaderBuilder(Path path, Schema schema) {
        ParquetReader.Builder<Row> builder = ParquetReader.builder(new RowReadSupport(schema), path);
        builder.useColumnIndexFilter(false);
        return builder;
    }
}
