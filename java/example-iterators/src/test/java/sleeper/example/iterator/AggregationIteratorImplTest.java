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
import sleeper.example.iterator.AggregationFilteringIterator.Aggregation;
import sleeper.example.iterator.AggregationFilteringIterator.AggregationOp;
import sleeper.example.iterator.AggregationFilteringIterator.FilterAggregationConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static sleeper.example.iterator.AggregatorIteratorImpl.aggregateOnTo;

public class AggregationIteratorImplTest {

    private static FilterAggregationConfig createConfig() {
        return new FilterAggregationConfig(List.of("key1", "key2", "sort_key", "sort_key2"), Optional.empty(),
                0,
                List.of());
    }

    private static FilterAggregationConfig createConfigWithAggregations() {
        return new FilterAggregationConfig(List.of("key1", "key2", "sort_key", "sort_key2"), Optional.empty(),
                0,
                List.of(new Aggregation("value1", AggregationOp.SUM),
                        new Aggregation("value2", AggregationOp.MIN)));
    }

    private static void checkAggregationResult(List<Record> records, List<Record> expectedResults) throws Exception {
        List<Record> results = aggregate(records);
        assertThat(results).containsExactlyElementsOf(expectedResults);
    }

    /**
     * Aggregate the given list.
     *
     * @param  records to aggregate
     * @return         aggregated list
     */
    private static List<Record> aggregate(List<Record> records) throws Exception {
        CloseableIterator<Record> it = new WrappedIterator<>(records.iterator());
        try (AggregatorIteratorImpl aggregator = new AggregatorIteratorImpl(createConfigWithAggregations(), it)) {
            List<Record> list = new ArrayList<>();
            aggregator.forEachRemaining(list::add);
            return list;
        }
    }

    @Test
    public void shouldAggregateZeroRecords() throws Exception {
        // Given
        List<Record> records = List.of();
        List<Record> expected = List.of();

        // Then
        checkAggregationResult(records, expected);
    }

    @Test
    public void shouldAggregateOneRecord() throws Exception {
        // Given
        Record r1 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 2.0d, "value1", "test", "value2", 12));

        List<Record> records = List.of(r1);
        List<Record> expected = List.of(new Record(r1));

        // Then
        checkAggregationResult(records, expected);
    }

    @Test
    public void shouldAggregateTwoEqualKeyRecords() throws Exception {
        // Given
        Record r1 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 2.0d, "value1", "test", "value2", 12));
        Record r2 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 2.0d, "value1", "test", "value2", 34));
        Record r3 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 2.0d, "value1", "testtest", "value2", 12));

        List<Record> records = List.of(r1, r2);
        List<Record> expected = List.of(r3);

        // Then
        checkAggregationResult(records, expected);
    }

    @Test
    public void shouldAggregateTwoDifferentKeyRecords() throws Exception {
        // Given
        Record r1 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 2.0d, "value1", "test", "value2", 12));
        Record r2 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 99999.0d, "value1", "test", "value2", 34));

        List<Record> records = List.of(r1, r2);
        List<Record> expected = List.of(new Record(r1), new Record(r2));

        // Then
        checkAggregationResult(records, expected);
    }

    @Test
    public void shouldAggregateTwoEqualThenOneDifferentRecord() throws Exception {
        // Given
        Record r1 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 2.0d, "value1", "test", "value2", 12));
        Record r2 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 2.0d, "value1", "test", "value2", 34));
        Record r3 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 9999999.0d, "value1", "test", "value2", 34));

        Record r4 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 2.0d, "value1", "testtest", "value2", 12));
        Record r5 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 9999999.0d, "value1", "test", "value2", 34));

        List<Record> records = List.of(r1, r2, r3);
        List<Record> expected = List.of(r4, r5);

        // Then
        checkAggregationResult(records, expected);
    }

    @Test
    public void shouldAggregateOneDifferentThenTwoEqualRecord() throws Exception {
        // Given
        Record r1 = new Record(Map.of("key1", 12, "key2", "other_value", "sort_key", true,
                "sort_key2", 2.0d, "value1", "test", "value2", 12));
        Record r2 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 2.0d, "value1", "test", "value2", 34));
        Record r3 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 2.0d, "value1", "test", "value2", 5));

        Record r4 = new Record(Map.of("key1", 12, "key2", "other_value", "sort_key", true,
                "sort_key2", 2.0d, "value1", "test", "value2", 12));
        Record r5 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 2.0d, "value1", "testtest", "value2", 5));

        List<Record> records = List.of(r1, r2, r3);
        List<Record> expected = List.of(r4, r5);

        // Then
        checkAggregationResult(records, expected);
    }

    @Test
    public void shouldAggregateTwoSameThenOneDifferentThenTwoEqualRecord() throws Exception {
        // Given
        Record r1 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 2.0d, "value1", "test", "value2", 12));
        Record r2 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 2.0d, "value1", "test", "value2", 34));

        Record r3 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 9999.0d, "value1", "test", "value2", 5));

        Record r4 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 5.0d, "value1", "test", "value2", 56));
        Record r5 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 5.0d, "value1", "test", "value2", 23));

        Record r6 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 2.0d, "value1", "testtest", "value2", 12));
        Record r7 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 9999.0d, "value1", "test", "value2", 5));
        Record r8 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 5.0d, "value1", "testtest", "value2", 23));
        List<Record> records = List.of(r1, r2, r3, r4, r5);

        List<Record> expected = List.of(r6, r7, r8);

        // Then
        checkAggregationResult(records, expected);
    }

    @Test
    public void shouldAggregateOneDifferentThenTwoSameThenOneDifferent() throws Exception {
        // Given
        Record r1 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 2.0d, "value1", "test", "value2", 12));

        Record r2 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 5.0d, "value1", "test", "value2", 76));
        Record r3 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 5.0d, "value1", "test", "value2", 56));

        Record r4 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 99999.0d, "value1", "test", "value2", 23));

        Record r5 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 2.0d, "value1", "test", "value2", 12));
        Record r6 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 5.0d, "value1", "testtest", "value2", 56));
        Record r7 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 99999.0d, "value1", "test", "value2", 23));
        List<Record> records = List.of(r1, r2, r3, r4);

        List<Record> expected = List.of(r5, r6, r7);

        // Then
        checkAggregationResult(records, expected);
    }

    @Test
    public void shouldThrowOnNullIterator() {
        assertThatNullPointerException().isThrownBy(() -> new AggregatorIteratorImpl(createConfig(), null));
    }

    @Test
    public void shouldThrowOnNullConfig() {
        assertThatNullPointerException().isThrownBy(() -> new AggregatorIteratorImpl(null,
                new WrappedIterator<Record>(List.<Record>of().iterator())));
    }

    @Test
    public void shouldAggregate() {
        // Given
        Record r1 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 5.6d, "value1", "test", "value2", 78));
        Record r2 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 5.6d, "value1", "testaaaaa", "value2", 7800));

        Record expected = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 5.6d, "value1", "testtestaaaaa", "value2", 78));
        // When
        aggregateOnTo(r1, r2, createConfigWithAggregations());

        // Then
        assertThat(r1).isEqualTo(expected);
    }

    @Test
    public void shouldReturnFirstRecordNoAggregations() {
        // Given
        Record r1 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 5.6d, "value1", "test", "value2", 78));
        Record r2 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 5.6d, "value1", "testaaaaa", "value2", 7800));

        Record expected = new Record(r1);
        // When
        aggregateOnTo(r1, r2, createConfig());

        // Then
        assertThat(r1).isEqualTo(expected);
    }

    @Test
    public void shouldBeEqualRecords() throws Exception {
        // Given
        Record r1 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 5.6d, "value1", "test", "value2", 78));
        Record r2 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 5.6d, "value1", "testaaaaa", "value2", 7800));
        AggregatorIteratorImpl iteratorImpl = new AggregatorIteratorImpl(createConfig(),
                new WrappedIterator<Record>(List.<Record>of().iterator()));

        // When
        boolean equal = iteratorImpl.recordsEqual(r1, r2);
        iteratorImpl.close();

        // Then
        assertThat(equal).isTrue();
    }

    @Test
    public void shouldBeNonEqualRecords() throws Exception {
        // Given
        Record r1 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 5.6d, "value1", "test", "value2", 78));
        // Make sort_key2 different
        Record r2 = new Record(Map.of("key1", 12, "key2", "test", "sort_key", true,
                "sort_key2", 5.7d, "value1", "testaaaaa", "value2", 7800));
        AggregatorIteratorImpl iteratorImpl = new AggregatorIteratorImpl(createConfig(),
                new WrappedIterator<Record>(List.<Record>of().iterator()));

        // When
        boolean equal = iteratorImpl.recordsEqual(r1, r2);
        iteratorImpl.close();

        // Then
        assertThat(equal).isFalse();
    }

}
