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

import org.apache.parquet.hadoop.ParquetReader;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Iterates through rows read from a Parquet file. Wraps a {@link ParquetReader}.
 */
public class ParquetReaderIterator implements CloseableIterator<Row> {
    private final ParquetReader<Row> reader;
    private Row row;
    private long rowsRead;

    public ParquetReaderIterator(ParquetReader<Row> reader) throws IOException {
        this.reader = reader;
        this.row = reader.read();
        this.rowsRead = 0L;
        if (null != this.row) {
            this.rowsRead++;
        }
    }

    @Override
    public boolean hasNext() {
        return null != row;
    }

    @Override
    public Row next() throws NoSuchElementException {
        if (!hasNext()) {
            return null;
        }
        Row copy = new Row(row);
        try {
            row = reader.read();
            if (null != row) {
                rowsRead++;
            }
        } catch (IOException e) {
            throw new RuntimeException("IOException when reading from ParquetReader: ", e);
        }
        return copy;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    public long getNumberOfRowsRead() {
        return rowsRead;
    }
}
