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

import sleeper.core.iterator.AggregationFilteringIterator.AggregationOp;
import sleeper.core.iterator.AggregationFilteringIterator.FilterAggregationConfig;
import sleeper.core.record.SleeperRow;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

public class AggregationFilteringIteratorTest {

    @SuppressWarnings("unchecked")
    @Test
    public void shouldApplySum() {
        // Given
        AggregationOp sum = AggregationOp.SUM;

        // When
        Object intResult = sum.apply(1, 2);
        Object longResult = sum.apply(1L, 2L);
        Object stringResult = sum.apply("one string", "two string");
        Object byteArrayResult = sum.apply(new byte[]{1, 2, 3, 4, 5}, new byte[]{6, 7, 8, 9, 10});
        Map<String, Integer> mapIntResult = (Map<String, Integer>) sum.apply(Map.of("key1", 1, "key2", 3), Map.of("key2", 4, "key3", 6));
        Map<String, Long> mapLongResult = (Map<String, Long>) sum.apply(Map.of("key1", 1L, "key2", 3L), Map.of("key2", 4L, "key3", 6L));
        Map<String, String> mapStringResult = (Map<String, String>) sum.apply(Map.of("key1", "test", "key2", "one string"), Map.of("key2", "two string", "key3", "other"));
        Map<String, byte[]> mapByteArrayResult = (Map<String, byte[]>) sum.apply(Map.of("key1", new byte[]{1, 2}, "key2", new byte[]{3, 4}),
                Map.of("key2", new byte[]{5, 6}, "key3", new byte[]{7, 8}));

        // Then
        assertThat(intResult).isEqualTo(3);
        assertThat(longResult).isEqualTo(3L);
        assertThat(stringResult).isEqualTo("one stringtwo string");
        assertThat(byteArrayResult).isEqualTo(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        assertThat(mapIntResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", 1, "key2", 7, "key3", 6));
        assertThat(mapLongResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", 1L, "key2", 7L, "key3", 6L));
        assertThat(mapStringResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", "test", "key2", "one stringtwo string", "key3", "other"));
        assertThat(mapByteArrayResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", new byte[]{1, 2}, "key2", new byte[]{3, 4, 5, 6}, "key3", new byte[]{7, 8}));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldApplyMin() {
        // Given
        AggregationOp sum = AggregationOp.MIN;

        // When
        Object intResult = sum.apply(1, 2);
        Object longResult = sum.apply(1L, 2L);
        Object stringResult = sum.apply("one string", "two string");
        Object byteArrayResult = sum.apply(new byte[]{1, 2, 3, 4, 5}, new byte[]{6, 7, 8, 9, 10});
        Map<String, Integer> mapIntResult = (Map<String, Integer>) sum.apply(Map.of("key1", 1, "key2", 3), Map.of("key2", 4, "key3", 6));
        Map<String, Long> mapLongResult = (Map<String, Long>) sum.apply(Map.of("key1", 1L, "key2", 3L), Map.of("key2", 4L, "key3", 6L));
        Map<String, String> mapStringResult = (Map<String, String>) sum.apply(Map.of("key1", "test", "key2", "one string"), Map.of("key2", "two string", "key3", "other"));
        Map<String, byte[]> mapByteArrayResult = (Map<String, byte[]>) sum.apply(Map.of("key1", new byte[]{1, 2}, "key2", new byte[]{3, 4}),
                Map.of("key2", new byte[]{5, 6}, "key3", new byte[]{7, 8}));

        // Then
        assertThat(intResult).isEqualTo(1);
        assertThat(longResult).isEqualTo(1L);
        assertThat(stringResult).isEqualTo("one string");
        assertThat(byteArrayResult).isEqualTo(new byte[]{1, 2, 3, 4, 5});
        assertThat(mapIntResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", 1, "key2", 3, "key3", 6));
        assertThat(mapLongResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", 1L, "key2", 3L, "key3", 6L));
        assertThat(mapStringResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", "test", "key2", "one string", "key3", "other"));
        assertThat(mapByteArrayResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", new byte[]{1, 2}, "key2", new byte[]{3, 4}, "key3", new byte[]{7, 8}));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldApplyMax() {
        // Given
        AggregationOp sum = AggregationOp.MAX;

        // When
        Object intResult = sum.apply(1, 2);
        Object longResult = sum.apply(1L, 2L);
        Object stringResult = sum.apply("one string", "two string");
        Object byteArrayResult = sum.apply(new byte[]{1, 2, 3, 4, 5}, new byte[]{6, 7, 8, 9, 10});
        Map<String, Integer> mapIntResult = (Map<String, Integer>) sum.apply(Map.of("key1", 1, "key2", 3), Map.of("key2", 4, "key3", 6));
        Map<String, Long> mapLongResult = (Map<String, Long>) sum.apply(Map.of("key1", 1L, "key2", 3L), Map.of("key2", 4L, "key3", 6L));
        Map<String, String> mapStringResult = (Map<String, String>) sum.apply(Map.of("key1", "test", "key2", "one string"), Map.of("key2", "two string", "key3", "other"));
        Map<String, byte[]> mapByteArrayResult = (Map<String, byte[]>) sum.apply(Map.of("key1", new byte[]{1, 2}, "key2", new byte[]{3, 4}),
                Map.of("key2", new byte[]{5, 6}, "key3", new byte[]{7, 8}));

        // Then
        assertThat(intResult).isEqualTo(2);
        assertThat(longResult).isEqualTo(2L);
        assertThat(stringResult).isEqualTo("two string");
        assertThat(byteArrayResult).isEqualTo(new byte[]{6, 7, 8, 9, 10});
        assertThat(mapIntResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", 1, "key2", 4, "key3", 6));
        assertThat(mapLongResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", 1L, "key2", 4L, "key3", 6L));
        assertThat(mapStringResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", "test", "key2", "two string", "key3", "other"));
        assertThat(mapByteArrayResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", new byte[]{1, 2}, "key2", new byte[]{5, 6}, "key3", new byte[]{7, 8}));
    }

    @Test
    public void shouldThrowOnDifferentOperandTypes() {
        // Given
        AggregationOp sum = AggregationOp.MAX;

        // Then
        assertThatIllegalArgumentException().isThrownBy(() -> {
            sum.apply(5, "not an integer");
        }).withMessage("different operands, lhs type: class java.lang.Integer rhs type: class java.lang.String");
    }

    @Test
    public void shouldThrowOnIllegalType() {
        // Given
        AggregationOp sum = AggregationOp.MAX;

        // Then
        assertThatIllegalArgumentException().isThrownBy(() -> {
            sum.apply(new Object(), new Object());
        }).withMessage("Value type not implemented class java.lang.Object");
    }

    @Test
    public void shouldThrowOnIllegalMapType() {
        // Given
        AggregationOp sum = AggregationOp.MAX;

        // Then
        assertThatIllegalArgumentException().isThrownBy(() -> {
            sum.apply(Map.of("key1", new Object()), Map.of("key2", new Object()));
        }).withMessage("Value type not implemented class java.lang.Object");
    }

    @Test
    public void shouldThrowOnEmptyGroupingColums() {
        assertThatIllegalArgumentException().isThrownBy(() -> {
            new FilterAggregationConfig(List.of(), Optional.empty(), 0, List.of());
        }).withMessage("must have at least one grouping column");
    }

    @Test
    public void shouldThrowOnNullGroupingColumns() {
        assertThatNullPointerException().isThrownBy(() -> {
            new FilterAggregationConfig(null, Optional.empty(), 0, List.of());
        }).withMessage("groupingColumns");
    }

    @Test
    public void shouldThrowOnNullFilter() {
        assertThatNullPointerException().isThrownBy(() -> {
            new FilterAggregationConfig(List.of("test"), null, 0, List.of());
        }).withMessage("ageOffColumn");
    }

    @Test
    public void shouldThrowOnNullAggregations() {
        assertThatNullPointerException().isThrownBy(() -> {
            new FilterAggregationConfig(List.of("test"), Optional.empty(), 0, null);
        }).withMessage("aggregations");
    }

    @Test
    public void shouldThrowOnUninitialised() {
        assertThatIllegalStateException().isThrownBy(() -> {
            // Given
            AggregationFilteringIterator afi = new AggregationFilteringIterator();
            afi.apply(null);
        }).withMessage("AggregatingIterator has not been initialised, call init()");
    }

    @Test
    public void shouldCreateAgeOffFilter() throws Exception {
        // Given
        AggregationFilteringIterator iterator = new AggregationFilteringIterator();
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("timestamp", new LongType()))
                .valueFields(new Field("value", new StringType()))
                .build();

        CloseableIterator<SleeperRow> recordIter = new WrappedIterator<>(List.of(
                new SleeperRow(Map.of("key", "a", "timestamp", 10L, "value", "test_value")),
                new SleeperRow(Map.of("key", "b", "timestamp", 999999999999999L, "value", "test_value2")),
                new SleeperRow(Map.of("key", "c", "timestamp", 10L, "value", "test_value3"))

        ).iterator());

        // When
        iterator.init(";ageoff=timestamp,10,", schema);
        CloseableIterator<SleeperRow> filtered = iterator.apply(recordIter);
        List<SleeperRow> result = new ArrayList<>();
        filtered.forEachRemaining(result::add);

        // Then
        assertThat(result).containsExactly(
                new SleeperRow(Map.of("key", "b", "timestamp", 999999999999999L, "value", "test_value2")));
    }

    @Test
    public void shouldReturnValueFields() {
        // Given
        AggregationFilteringIterator iterator = new AggregationFilteringIterator();
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
        AggregationFilteringIterator iterator = new AggregationFilteringIterator();
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
        AggregationFilteringIterator iterator = new AggregationFilteringIterator();
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
        AggregationFilteringIterator iterator = new AggregationFilteringIterator();
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
        AggregationFilteringIterator iterator = new AggregationFilteringIterator();
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
        AggregationFilteringIterator iterator = new AggregationFilteringIterator();
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
        AggregationFilteringIterator iterator = new AggregationFilteringIterator();
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
        AggregationFilteringIterator iterator = new AggregationFilteringIterator();
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
    public void shouldAggregateZeroRecords() throws Exception {
        // Given
        List<SleeperRow> records = List.of();
        List<SleeperRow> expected = List.of();

        // Then
        assertThat(aggregate(records)).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldAggregateOneRecord() throws Exception {
        // Given
        SleeperRow r1 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 5,
                "sort_key2", 2, "value1", "test", "value2", 12));

        List<SleeperRow> records = List.of(r1);
        List<SleeperRow> expected = List.of(new SleeperRow(r1));

        // Then
        assertThat(aggregate(records)).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldAggregateTwoEqualKeyRecords() throws Exception {
        // Given
        SleeperRow r1 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 12));
        SleeperRow r2 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 34));

        SleeperRow r3 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "testtest", "value2", 12));

        List<SleeperRow> records = List.of(r1, r2);
        List<SleeperRow> expected = List.of(r3);

        // Then
        assertThat(aggregate(records)).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldAggregateTwoDifferentKeyRecords() throws Exception {
        // Given
        SleeperRow r1 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 12));
        SleeperRow r2 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 99999, "value1", "test", "value2", 34));

        List<SleeperRow> records = List.of(r1, r2);
        List<SleeperRow> expected = List.of(new SleeperRow(r1), new SleeperRow(r2));

        // Then
        assertThat(aggregate(records)).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldAggregateTwoEqualThenOneDifferentRecord() throws Exception {
        // Given
        SleeperRow r1 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 12));
        SleeperRow r2 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 34));
        SleeperRow r3 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 9999999, "value1", "test", "value2", 34));

        SleeperRow r4 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "testtest", "value2", 12));
        SleeperRow r5 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 9999999, "value1", "test", "value2", 34));

        List<SleeperRow> records = List.of(r1, r2, r3);
        List<SleeperRow> expected = List.of(r4, r5);

        // Then
        assertThat(aggregate(records)).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldAggregateOneDifferentThenTwoEqualRecord() throws Exception {
        // Given
        SleeperRow r1 = new SleeperRow(Map.of("key1", 12, "key2", "other_value", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 12));
        SleeperRow r2 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 34));
        SleeperRow r3 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 5));

        SleeperRow r4 = new SleeperRow(Map.of("key1", 12, "key2", "other_value", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 12));
        SleeperRow r5 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "testtest", "value2", 5));

        List<SleeperRow> records = List.of(r1, r2, r3);
        List<SleeperRow> expected = List.of(r4, r5);

        // Then
        assertThat(aggregate(records)).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldAggregateTwoSameThenOneDifferentThenTwoEqualRecord() throws Exception {
        // Given
        SleeperRow r1 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 12));
        SleeperRow r2 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 34));

        SleeperRow r3 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 9999, "value1", "test", "value2", 5));

        SleeperRow r4 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "test", "value2", 56));
        SleeperRow r5 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "test", "value2", 23));

        SleeperRow r6 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "testtest", "value2", 12));
        SleeperRow r7 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 9999, "value1", "test", "value2", 5));
        SleeperRow r8 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "testtest", "value2", 23));
        List<SleeperRow> records = List.of(r1, r2, r3, r4, r5);

        List<SleeperRow> expected = List.of(r6, r7, r8);

        // Then
        assertThat(aggregate(records)).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldAggregateOneDifferentThenTwoSameThenOneDifferent() throws Exception {
        // Given
        SleeperRow r1 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 12));

        SleeperRow r2 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "test", "value2", 76));
        SleeperRow r3 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "test", "value2", 56));

        SleeperRow r4 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 99999, "value1", "test", "value2", 23));

        SleeperRow r5 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 2, "value1", "test", "value2", 12));
        SleeperRow r6 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "testtest", "value2", 56));
        SleeperRow r7 = new SleeperRow(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 99999, "value1", "test", "value2", 23));
        List<SleeperRow> records = List.of(r1, r2, r3, r4);

        List<SleeperRow> expected = List.of(r5, r6, r7);

        // Then
        assertThat(aggregate(records)).containsExactlyElementsOf(expected);
    }

    /**
     * Aggregate the given list.
     *
     * @param  records to aggregate
     * @return         aggregated list
     */
    private static List<SleeperRow> aggregate(List<SleeperRow> records) throws Exception {
        CloseableIterator<SleeperRow> it = new WrappedIterator<>(records.iterator());

        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key1", new IntType()), new Field("key2", new StringType()))
                .sortKeyFields(new Field("sort_key", new IntType()), new Field("sort_key2", new IntType()))
                .valueFields(new Field("value1", new StringType()), new Field("value2", new IntType()))
                .build();
        AggregationFilteringIterator aggregationIterator = new AggregationFilteringIterator();
        aggregationIterator.init("sort_key,sort_key2;,sum(value1),min(value2)", schema);

        try (CloseableIterator<SleeperRow> aggregator = aggregationIterator.apply(it)) {
            List<SleeperRow> list = new ArrayList<>();
            aggregator.forEachRemaining(list::add);
            return list;
        }
    }
}
