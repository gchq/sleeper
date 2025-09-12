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
package sleeper.core.iterator;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;

import java.util.List;

/**
 * An iterator that applies multiple other iterators to a sequence of rows.
 */
public class SortedRowIterators implements SortedRowIterator {

    private final List<SortedRowIterator> iterators;
    private final List<String> requiredValueFields;

    public SortedRowIterators(List<SortedRowIterator> iterators) {
        this.iterators = iterators;
        this.requiredValueFields = iterators.stream()
                .flatMap(iterator -> iterator.getRequiredValueFields().stream())
                .distinct()
                .toList();
    }

    @Override
    public CloseableIterator<Row> apply(CloseableIterator<Row> input) {
        CloseableIterator<Row> rows = input;
        for (SortedRowIterator iterator : iterators) {
            rows = iterator.apply(rows);
        }
        return rows;
    }

    @Override
    public List<String> getRequiredValueFields() {
        return requiredValueFields;
    }

}
