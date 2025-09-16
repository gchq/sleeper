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

import org.junit.jupiter.api.Test;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.iterator.closeable.WrappedIterator;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class ConfigStringAggregationFilteringIteratorTest {
    @Test
    public void shouldCreateAgeOffFilter() throws Exception {
        // Given
        ConfigStringAggregationFilteringIterator iterator = new ConfigStringAggregationFilteringIterator();
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("timestamp", new LongType()))
                .valueFields(new Field("value", new StringType()))
                .build();

        CloseableIterator<Row> rowIter = new WrappedIterator<>(List.of(
                new Row(Map.of("key", "a", "timestamp", 10L, "value", "test_value")),
                new Row(Map.of("key", "b", "timestamp", 999999999999999L, "value", "test_value2")),
                new Row(Map.of("key", "c", "timestamp", 10L, "value", "test_value3"))

        ).iterator());

        // When
        iterator.init(";ageoff=timestamp,10,", schema);
        CloseableIterator<Row> filtered = iterator.applyTransform(rowIter);
        List<Row> result = new ArrayList<>();
        filtered.forEachRemaining(result::add);

        // Then
        assertThat(result).containsExactly(
                new Row(Map.of("key", "b", "timestamp", 999999999999999L, "value", "test_value2")));
    }

    @Test
    public void shouldReturnValueFields() {
        // Given
        ConfigStringAggregationFilteringIterator iterator = new ConfigStringAggregationFilteringIterator();
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("timestamp", new LongType()))
                .valueFields(new Field("value", new StringType()))
                .build();

        // When
        iterator.init("timestamp;,min(value)", schema);

        // Then
        assertThat(iterator.getRequiredValueFields()).containsExactly("key", "timestamp", "value");
    }

    @Test
    public void shouldFailValidationWhenGroupingByRowKey() {
        // Given
        ConfigStringAggregationFilteringIterator iterator = new ConfigStringAggregationFilteringIterator();
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("timestamp", new LongType()))
                .valueFields(new Field("value", new StringType()))
                .build();

        // Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> {
                    iterator.init("key;,sum(value)", schema);
                })
                .withMessage("Aggregation grouping column key is already a row key column or is duplicated");
    }

    @Test
    public void shouldFailValidationWhenDuplicatingGroupBySortKey() {
        // Given
        ConfigStringAggregationFilteringIterator iterator = new ConfigStringAggregationFilteringIterator();
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("timestamp", new LongType()))
                .valueFields(new Field("value", new StringType()))
                .build();

        // Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> {
                    iterator.init("timestamp,timestamp;,sum(value)", schema);
                })
                .withMessage("Aggregation grouping column timestamp is already a row key column or is duplicated");
    }

    @Test
    public void shouldFailValidationWhenGroupingByNonExistentColumn() {
        // Given
        ConfigStringAggregationFilteringIterator iterator = new ConfigStringAggregationFilteringIterator();
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("timestamp", new LongType()))
                .valueFields(new Field("value", new StringType()))
                .build();

        // Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> {
                    iterator.init("not_there;,sum(value)", schema);
                })
                .withMessage("Aggregation grouping column not_there doesn't exist");
    }

    @Test
    public void shouldFailValidationWhenAggregatingRowKey() {
        // Given
        ConfigStringAggregationFilteringIterator iterator = new ConfigStringAggregationFilteringIterator();
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("timestamp", new LongType()))
                .valueFields(new Field("value", new StringType()))
                .build();

        // Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> {
                    iterator.init(";,sum(value),sum(key),sum(timestamp)", schema);
                })
                .withMessage("Row key/extra grouping column key cannot have an aggregation");
    }

    @Test
    public void shouldFailValidationWhenAggregatingNonExistentColumn() {
        // Given
        ConfigStringAggregationFilteringIterator iterator = new ConfigStringAggregationFilteringIterator();
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("timestamp", new LongType()))
                .valueFields(new Field("value", new StringType()))
                .build();

        // Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> {
                    iterator.init(";,sum(value),sum(not_there),sum(timestamp)", schema);
                })
                .withMessage("Aggregation column not_there doesn't exist");
    }

    @Test
    public void shouldFailValidationWhenAggregatingColumnTwice() {
        // Given
        ConfigStringAggregationFilteringIterator iterator = new ConfigStringAggregationFilteringIterator();
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("timestamp", new LongType()))
                .valueFields(new Field("value", new StringType()))
                .build();

        // Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> {
                    iterator.init(";,sum(value),sum(value),sum(timestamp)", schema);
                })
                .withMessage("Aggregation column value duplicated");
    }

    @Test
    public void shouldFailValidationWhenOneColumnIsNotAggregated() {
        // Given
        ConfigStringAggregationFilteringIterator iterator = new ConfigStringAggregationFilteringIterator();
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("timestamp", new LongType()))
                .valueFields(new Field("value", new StringType()))
                .build();

        // Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> {
                    iterator.init(";,sum(value)", schema);
                })
                .withMessage("Column timestamp doesn't have a aggregation operator specified");
    }

    @Test
    public void shouldAggregateZeroRows() throws Exception {
        // Given
        List<Row> rows = List.of();
        List<Row> expected = List.of();

        // Then
        assertThat(aggregate(rows)).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldAggregateOneRow() throws Exception {
        // Given
        Row r1 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 5,
                "sort_key2", 2, "value1", "test", "value2", 12));

        List<Row> rows = List.of(r1);
        List<Row> expected = List.of(new Row(r1));

        // Then
        assertThat(aggregate(rows)).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldAggregateTwoEqualKeyRows() throws Exception {
        // Given
        Row r1 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 12));
        Row r2 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 34));

        Row r3 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "testtest", "value2", 12));

        List<Row> rows = List.of(r1, r2);
        List<Row> expected = List.of(r3);

        // Then
        assertThat(aggregate(rows)).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldAggregateTwoDifferentKeyRows() throws Exception {
        // Given
        Row r1 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 12));
        Row r2 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 99999, "value1", "test", "value2", 34));

        List<Row> rows = List.of(r1, r2);
        List<Row> expected = List.of(new Row(r1), new Row(r2));

        // Then
        assertThat(aggregate(rows)).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldAggregateTwoEqualThenOneDifferentRow() throws Exception {
        // Given
        Row r1 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 12));
        Row r2 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 34));
        Row r3 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 9999999, "value1", "test", "value2", 34));

        Row r4 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "testtest", "value2", 12));
        Row r5 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 9999999, "value1", "test", "value2", 34));

        List<Row> rows = List.of(r1, r2, r3);
        List<Row> expected = List.of(r4, r5);

        // Then
        assertThat(aggregate(rows)).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldAggregateOneDifferentThenTwoEqualRow() throws Exception {
        // Given
        Row r1 = new Row(Map.of("key1", 12, "key2", "other_value", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 12));
        Row r2 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 34));
        Row r3 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 5));

        Row r4 = new Row(Map.of("key1", 12, "key2", "other_value", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 12));
        Row r5 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "testtest", "value2", 5));

        List<Row> rows = List.of(r1, r2, r3);
        List<Row> expected = List.of(r4, r5);

        // Then
        assertThat(aggregate(rows)).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldAggregateTwoSameThenOneDifferentThenTwoEqualRow() throws Exception {
        // Given
        Row r1 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 12));
        Row r2 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 34));

        Row r3 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 9999, "value1", "test", "value2", 5));

        Row r4 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "test", "value2", 56));
        Row r5 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "test", "value2", 23));

        Row r6 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "testtest", "value2", 12));
        Row r7 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 9999, "value1", "test", "value2", 5));
        Row r8 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "testtest", "value2", 23));
        List<Row> rows = List.of(r1, r2, r3, r4, r5);

        List<Row> expected = List.of(r6, r7, r8);

        // Then
        assertThat(aggregate(rows)).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldAggregateOneDifferentThenTwoSameThenOneDifferent() throws Exception {
        // Given
        Row r1 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 12));

        Row r2 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "test", "value2", 76));
        Row r3 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "test", "value2", 56));

        Row r4 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 99999, "value1", "test", "value2", 23));

        Row r5 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 12));
        Row r6 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "testtest", "value2", 56));
        Row r7 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 99999, "value1", "test", "value2", 23));
        List<Row> rows = List.of(r1, r2, r3, r4);

        List<Row> expected = List.of(r5, r6, r7);

        // Then
        assertThat(aggregate(rows)).containsExactlyElementsOf(expected);
    }

    /**
     * Aggregate the given list.
     *
     * @param  rows to aggregate
     * @return      aggregated list
     */
    private static List<Row> aggregate(List<Row> rows) throws Exception {
        CloseableIterator<Row> it = new WrappedIterator<>(rows.iterator());

        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key1", new IntType()), new Field("key2", new StringType()))
                .sortKeyFields(new Field("sort_key", new IntType()), new Field("sort_key2", new IntType()))
                .valueFields(new Field("value1", new StringType()), new Field("value2", new IntType()))
                .build();
        ConfigStringAggregationFilteringIterator aggregationIterator = new ConfigStringAggregationFilteringIterator();
        aggregationIterator.init("sort_key,sort_key2;,sum(value1),min(value2)", schema);

        try (CloseableIterator<Row> aggregator = aggregationIterator.applyTransform(it)) {
            List<Row> list = new ArrayList<>();
            aggregator.forEachRemaining(list::add);
            return list;
        }
    }
}
