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
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;

import java.io.IOException;

/**
 * Factory to create parquet reader for sleeper rows. Uses {@link RowReadSupport}.
 */
public class ParquetRowReaderFactory {

    private ParquetRowReaderFactory() {
    }

    /**
     * Builds a ParquetReader.Builder for reading rows from a parquet file with a known schema at a given path.
     *
     * @param  path   the path to the file
     * @param  schema the schema of the file
     * @return        a ParquetReader.Builder
     */
    public static ParquetReader.Builder<Row> parquetRowReaderBuilder(Path path, Schema schema) {
        ParquetReader.Builder<Row> builder = ParquetReader.builder(new RowReadSupport(schema), path);
        builder.useColumnIndexFilter(false);
        return builder;
    }

    /**
     * Reads rows from a Parquet file without knowing its schema in advance. The fields in each row are taken from the
     * schema held in the file. The returned iterator must be closed after use.
     *
     * @param  path        the path to the file
     * @param  conf        the Hadoop configuration
     * @return             an iterator over the rows in the file
     * @throws IOException if the file could not be opened
     */
    public static CloseableIterator<Row> parquetGenericRowIterator(Path path, Configuration conf) throws IOException {
        return new ParquetGenericRowIterator(path, conf);
    }
}
