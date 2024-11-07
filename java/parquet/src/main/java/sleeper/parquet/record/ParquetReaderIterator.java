/*
 * Copyright 2022-2024 Crown Copyright
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

import org.apache.parquet.hadoop.ParquetReader;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.record.Record;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Iterates through records read from a Parquet file. Wraps a {@link ParquetReader}.
 */
public class ParquetReaderIterator implements CloseableIterator<Record> {
    private final ParquetReader<Record> reader;
    private Record record;
    private long recordsRead;

    public ParquetReaderIterator(ParquetReader<Record> reader) throws IOException {
        this.reader = reader;
        this.record = reader.read();
        this.recordsRead = 0L;
        if (null != this.record) {
            this.recordsRead++;
        }
    }

    @Override
    public boolean hasNext() {
        return null != record;
    }

    @Override
    public Record next() throws NoSuchElementException {
        if (!hasNext()) {
            return null;
        }
        Record copy = new Record(record);
        try {
            record = reader.read();
            if (null != record) {
                recordsRead++;
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

    public long getNumberOfRecordsRead() {
        return recordsRead;
    }
}
