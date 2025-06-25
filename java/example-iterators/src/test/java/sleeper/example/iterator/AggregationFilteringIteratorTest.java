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
package sleeper.example.iterator;

import org.junit.jupiter.api.Test;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.example.iterator.AggregationFilteringIterator.AggregationOp;
import sleeper.example.iterator.AggregationFilteringIterator.FilterAggregationConfig;

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
    public void testSumApply() {
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
    public void testMinApply() {
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
    public void testMaxApply() {
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

        CloseableIterator<Record> recordIter = new WrappedIterator<>(List.of(
                new Record(Map.of("key", "a", "timestamp", 10L, "value", "test_value")),
                new Record(Map.of("key", "b", "timestamp", 999999999999999L, "value", "test_value2")),
                new Record(Map.of("key", "c", "timestamp", 10L, "value", "test_value3"))

        ).iterator());

        // When
        iterator.init(";ageoff=timestamp,10,", schema);
        CloseableIterator<Record> filtered = iterator.apply(recordIter);
        List<Record> result = new ArrayList<>();
        filtered.forEachRemaining(result::add);

        // Then
        assertThat(result).containsExactly(
                new Record(Map.of("key", "b", "timestamp", 999999999999999L, "value", "test_value2")));
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
    public void validateCheckDuplicateGroupingColumnRowKey() {
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
    public void validateCheckDuplicateGroupingColumnSortKey() {
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
    public void validateCheckNonExistantColumn() {
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
    public void validateCheckRowKeyCantHaveAggregation() {
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
    public void validateCheckAggregationColumnNonExistant() {
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
    public void validateCheckAggregationColumnDuplicated() {
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
    public void validateCheckAllAggregationsSpecified() {
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
}
