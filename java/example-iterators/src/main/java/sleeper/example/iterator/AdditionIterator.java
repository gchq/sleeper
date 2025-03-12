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
import sleeper.core.key.Key;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Combines records with identical row keys and sort keys by summing the values in each column. It assumes that all
 * value fields are longs. This is an example implementation of a {@link SortedRecordIterator}. This implementation
 * is very generic and is provided as an example. More efficient implementations can be written for any specific
 * schema, e.g. by directly comparing the key and sort fields in the equalRowAndSort method, rather than explicitly
 * constructing a {@link Key} object.
 */
public class AdditionIterator implements SortedRecordIterator {
    private List<String> rowKeyFieldNames;
    private List<String> sortKeyFieldNames;
    private List<String> valueFieldNames;

    public AdditionIterator() {
    }

    @Override
    public void init(String configString, Schema schema) {
        this.rowKeyFieldNames = schema.getRowKeyFieldNames();
        this.sortKeyFieldNames = schema.getSortKeyFieldNames();
        this.valueFieldNames = schema.getValueFieldNames();
    }

    @Override
    public List<String> getRequiredValueFields() {
        return valueFieldNames;
    }

    @Override
    public CloseableIterator<Record> apply(CloseableIterator<Record> input) {
        return new AdditionIteratorInternal(input, rowKeyFieldNames, sortKeyFieldNames, valueFieldNames);
    }

    /**
     * Sums values for identical row and sort keys in the input iterator.
     */
    public static class AdditionIteratorInternal implements CloseableIterator<Record> {
        private final CloseableIterator<Record> input;
        private final List<String> rowKeyFieldNames;
        private final List<String> sortKeyFieldNames;
        private final List<String> valueFieldNames;
        private Record current;

        public AdditionIteratorInternal(CloseableIterator<Record> input,
                List<String> rowKeyFieldNames,
                List<String> sortKeyFieldNames,
                List<String> valueFieldNames) {
            this.input = input;
            this.rowKeyFieldNames = rowKeyFieldNames;
            this.sortKeyFieldNames = sortKeyFieldNames;
            this.valueFieldNames = valueFieldNames;
            this.current = getNextRecord();
        }

        @Override
        public boolean hasNext() {
            return null != current;
        }

        @Override
        public Record next() {
            Record record = new Record(current);
            Record next = getNextRecord();
            while (null != next && equalRowAndSort(current, next)) {
                for (String fieldName : valueFieldNames) {
                    Long number1 = (Long) record.get(fieldName);
                    Long number2 = (Long) next.get(fieldName);
                    record.put(fieldName, number1 + number2);
                }
                next = getNextRecord();
            }
            current = next;
            return record;
        }

        @Override
        public void close() throws IOException {
            input.close();
        }

        private Record getNextRecord() {
            if (input.hasNext()) {
                return input.next();
            }
            return null;
        }

        private boolean equalRowAndSort(Record record1, Record record2) {
            List<Object> keys1 = new ArrayList<>();
            List<Object> keys2 = new ArrayList<>();
            for (String rowKey : rowKeyFieldNames) {
                keys1.add(record1.get(rowKey));
                keys2.add(record2.get(rowKey));
            }
            for (String sortKey : sortKeyFieldNames) {
                keys1.add(record1.get(sortKey));
                keys2.add(record2.get(sortKey));
            }
            return Key.create(keys1).equals(Key.create(keys2));
        }
    }
}
