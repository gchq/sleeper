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
package sleeper.core.iterator.testutil;

import sleeper.core.iterator.SortedRowIterator;
import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.iterator.closeable.FilteringIterator;
import sleeper.core.iterator.closeable.LimitingIterator;
import sleeper.core.iterator.closeable.WrappedIterator;
import sleeper.core.row.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Helpers for working with SortedRowIterator.
 */
public class SortedRowIteratorTestHelper {

    private SortedRowIteratorTestHelper() {
    }

    /**
     * Applies an iterator to some data.
     *
     * @param  iterator the iterator
     * @param  rows     the input data
     * @return          the output data
     */
    public static List<Row> apply(SortedRowIterator iterator, List<Row> rows) {
        List<Row> output = new ArrayList<>();
        iterator.applyTransform(new WrappedIterator<>(rows.iterator()))
                .forEachRemaining(output::add);
        return output;
    }

    /**
     * Creates an iterator that filters out any fields that do not have a particular value for a specific value field.
     *
     * @param  field the field name
     * @param  value the value to match
     * @return       the iterator
     */
    public static SortedRowIterator filterOnValueField(String field, Object value) {
        return withRequiredValueFields(List.of(field),
                input -> new FilteringIterator<>(input, row -> Objects.equals(value, row.get(field))));
    }

    /**
     * Creates an iterator that only returns the first n rows.
     *
     * @param  limit the number of rows to return
     * @return       the iterator
     */
    public static SortedRowIterator limitRows(int limit) {
        return withNoRequiredValueFields(input -> new LimitingIterator<>(limit, input));
    }

    /**
     * Creates an iterator that requests specific value fields, and does not change the data.
     *
     * @param  requiredValueFields the required value fields
     * @return                     the iterator
     */
    public static SortedRowIterator withRequiredValueFields(List<String> requiredValueFields) {
        return withRequiredValueFields(requiredValueFields, input -> input);
    }

    /**
     * Creates an iterator that performs some operation on the underlying data, but does not require any extra value
     * fields to be read.
     *
     * @param  apply the operation to apply
     * @return       the iterator
     */
    public static SortedRowIterator withNoRequiredValueFields(Function<CloseableIterator<Row>, CloseableIterator<Row>> apply) {
        return withRequiredValueFields(List.of(), apply);
    }

    /**
     * Creates an iterator based on which value fields need to be read, and an operation to perform on the underlying
     * data.
     *
     * @param  requiredValueFields the required value fields
     * @param  apply               the operation to apply
     * @return                     the iterator
     */
    public static SortedRowIterator withRequiredValueFields(List<String> requiredValueFields, Function<CloseableIterator<Row>, CloseableIterator<Row>> apply) {
        return new SortedRowIterator() {
            @Override
            public CloseableIterator<Row> applyTransform(CloseableIterator<Row> input) {
                return apply.apply(input);
            }

            @Override
            public List<String> getRequiredValueFields() {
                return requiredValueFields;
            }
        };
    }
}
