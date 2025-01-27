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
package sleeper.example.iterator;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.SortedRecordIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Filters out records older than a specified age. If the specified timestamp field is more than a certain length of
 * time ago then the record is removed. This is an example implementation of {@link SortedRecordIterator}.
 */
public class AgeOffIterator implements SortedRecordIterator {
    private String fieldName;
    private long ageOff;

    public AgeOffIterator() {
    }

    @Override
    public void init(String configString, Schema schema) {
        String[] fields = configString.split(",");
        if (2 != fields.length) {
            throw new IllegalArgumentException("Configuration string should have 2 fields: field name and age off time");
        }
        fieldName = fields[0];
        ageOff = Long.parseLong(fields[1]);
    }

    @Override
    public List<String> getRequiredValueFields() {
        return Collections.singletonList(fieldName);
    }

    @Override
    public CloseableIterator<Record> apply(CloseableIterator<Record> input) {
        return new AgeOffIteratorInternal(input, fieldName, ageOff);
    }

    /**
     * Discards records in the input iterator when the timestamp is older than the limit.
     */
    public static class AgeOffIteratorInternal implements CloseableIterator<Record> {
        private final CloseableIterator<Record> input;
        private final String fieldName;
        private final long age;
        private Record next;

        public AgeOffIteratorInternal(
                CloseableIterator<Record> input,
                String fieldName,
                long age) {
            this.input = input;
            this.fieldName = fieldName;
            this.age = age;
            this.next = null;
            advance();
        }

        @Override
        public boolean hasNext() {
            return null != next;
        }

        @Override
        public Record next() {
            Record record = new Record(next);
            if (!input.hasNext()) {
                next = null;
            }
            advance();
            return record;
        }

        @Override
        public void close() throws IOException {
            input.close();
        }

        private void advance() {
            while (input.hasNext()) {
                next = input.next();
                Long value = (Long) next.get(fieldName);
                if (null != value && System.currentTimeMillis() - value < age) {
                    break;
                } else {
                    next = null;
                }
            }
        }
    }
}
