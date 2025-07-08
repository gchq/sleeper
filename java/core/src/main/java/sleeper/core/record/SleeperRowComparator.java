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
package sleeper.core.record;

import sleeper.core.key.Key;
import sleeper.core.schema.Schema;

import java.util.Comparator;
import java.util.List;

/**
 * Compares records by row keys then sort keys.
 */
public class SleeperRowComparator implements Comparator<SleeperRow> {
    private final List<String> rowKeyNames;
    private final List<String> sortKeyNames;
    private final KeyComparator rowKeyComparator;
    private final KeyComparator sortKeyComparator;

    public SleeperRowComparator(Schema schema) {
        this.rowKeyNames = schema.getRowKeyFieldNames();
        this.sortKeyNames = schema.getSortKeyFieldNames();
        this.rowKeyComparator = new KeyComparator(schema.getRowKeyTypes());
        this.sortKeyComparator = new KeyComparator(schema.getSortKeyTypes());
    }

    // TODO Optimise by avoiding creating lists of row keys and sort keys, and
    // just do the comparison directly here?
    @Override
    public int compare(SleeperRow record1, SleeperRow record2) {
        List<Object> record1Key = record1.getValues(rowKeyNames);
        List<Object> record2Key = record2.getValues(rowKeyNames);
        int keyComparison = rowKeyComparator.compare(Key.create(record1Key), Key.create(record2Key));
        if (0 != keyComparison) {
            return keyComparison;
        }
        List<Object> record1SortFields = record1.getValues(sortKeyNames);
        List<Object> record2SortFields = record2.getValues(sortKeyNames);
        return sortKeyComparator.compare(Key.create(record1SortFields), Key.create(record2SortFields));
    }
}
