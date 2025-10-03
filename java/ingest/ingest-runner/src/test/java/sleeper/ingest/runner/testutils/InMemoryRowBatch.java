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

package sleeper.ingest.runner.testutils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.iterator.closeable.WrappedIterator;
import sleeper.core.row.Row;
import sleeper.core.row.RowComparator;
import sleeper.core.rowbatch.RowBatch;
import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.List;

public class InMemoryRowBatch implements RowBatch<Row> {
    public static final Logger LOGGER = LoggerFactory.getLogger(InMemoryRowBatch.class);

    private final Schema schema;
    private final List<Row> rows = new ArrayList<>();

    public InMemoryRowBatch(Schema schema) {
        this.schema = schema;
    }

    @Override
    public void append(Row data) {
        rows.add(data);
    }

    @Override
    public boolean isFull() {
        return false;
    }

    @Override
    public CloseableIterator<Row> createOrderedRowIterator() {
        rows.sort(new RowComparator(schema));
        return new WrappedIterator<>(rows.iterator());
    }

    @Override
    public void close() {
        LOGGER.info("Closing batch with {} rows", rows.size());
    }
}
