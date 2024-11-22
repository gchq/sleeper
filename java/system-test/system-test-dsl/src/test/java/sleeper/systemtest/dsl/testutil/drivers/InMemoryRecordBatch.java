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

package sleeper.systemtest.dsl.testutil.drivers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.record.Record;
import sleeper.core.record.RecordComparator;
import sleeper.core.schema.Schema;
import sleeper.ingest.runner.impl.recordbatch.RecordBatch;

import java.util.ArrayList;
import java.util.List;

public class InMemoryRecordBatch implements RecordBatch<Record> {
    public static final Logger LOGGER = LoggerFactory.getLogger(InMemoryRecordBatch.class);

    private final Schema schema;
    private final List<Record> records = new ArrayList<>();

    public InMemoryRecordBatch(Schema schema) {
        this.schema = schema;
    }

    @Override
    public void append(Record data) {
        records.add(data);
    }

    @Override
    public boolean isFull() {
        return false;
    }

    @Override
    public CloseableIterator<Record> createOrderedRecordIterator() {
        records.sort(new RecordComparator(schema));
        return new WrappedIterator<>(records.iterator());
    }

    @Override
    public void close() {
        LOGGER.info("Closing batch with {} records", records.size());
    }
}
