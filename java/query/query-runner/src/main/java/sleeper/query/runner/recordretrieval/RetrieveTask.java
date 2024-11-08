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
package sleeper.query.runner.recordretrieval;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.hadoop.ParquetReader;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.record.Record;
import sleeper.parquet.record.ParquetReaderIterator;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Creates an iterator and pulls back the first element from that iterator. This is a relatively expensive process and
 * so should be done in parallel.
 */
public class RetrieveTask implements Callable<Pair<Record, CloseableIterator<Record>>> {
    private final ParquetReader<Record> reader;

    public RetrieveTask(ParquetReader<Record> reader) {
        this.reader = reader;
    }

    @Override
    public Pair<Record, CloseableIterator<Record>> call() {
        CloseableIterator<Record> iterator;
        try {
            iterator = new ParquetReaderIterator(reader);
        } catch (IOException e) {
            throw new RuntimeException("IOException creating ParquetReaderIterator", e);
        }
        if (iterator.hasNext()) {
            return new ImmutablePair<>(iterator.next(), iterator);
        }
        return null;
    }
}
