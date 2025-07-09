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

import sleeper.core.iterator.AggregationFilteringIterator.Aggregation;
import sleeper.core.iterator.AggregationFilteringIterator.AggregationOp;
import sleeper.core.iterator.AggregationFilteringIterator.FilterAggregationConfig;
import sleeper.core.record.Row;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static sleeper.core.iterator.AggregatorIteratorImpl.aggregateOnTo;

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

    @Test
    public void shouldThrowOnNullIterator() {
        assertThatNullPointerException().isThrownBy(() -> new AggregatorIteratorImpl(createConfig(), null));
    }

    @Test
    public void shouldThrowOnNullConfig() {
        assertThatNullPointerException().isThrownBy(() -> new AggregatorIteratorImpl(null,
                new WrappedIterator<Row>(List.<Row>of().iterator())));
    }

    @Test
    public void shouldAggregate() {
        // Given
        Row r1 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "test", "value2", 78));
        Row r2 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "testaaaaa", "value2", 7800));

        Row expected = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "testtestaaaaa", "value2", 78));
        // When
        aggregateOnTo(r1, r2, createConfigWithAggregations());

        // Then
        assertThat(r1).isEqualTo(expected);
    }

    @Test
    public void shouldReturnFirstRecordNoAggregations() {
        // Given
        Row r1 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "test", "value2", 78));
        Row r2 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "testaaaaa", "value2", 7800));

        Row expected = new Row(r1);
        // When
        aggregateOnTo(r1, r2, createConfig());

        // Then
        assertThat(r1).isEqualTo(expected);
    }

    @Test
    public void shouldBeEqualRecords() throws Exception {
        // Given
        Row r1 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "test", "value2", 78));
        Row r2 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "testaaaaa", "value2", 7800));
        AggregatorIteratorImpl iteratorImpl = new AggregatorIteratorImpl(createConfig(),
                new WrappedIterator<Row>(List.<Row>of().iterator()));

        // When
        boolean equal = iteratorImpl.recordsEqual(r1, r2);
        iteratorImpl.close();

        // Then
        assertThat(equal).isTrue();
    }

    @Test
    public void shouldBeNonEqualRecords() throws Exception {
        // Given
        Row r1 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "test", "value2", 78));
        // Make sort_key2 different
        Row r2 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 6, "value1", "testaaaaa", "value2", 7800));
        AggregatorIteratorImpl iteratorImpl = new AggregatorIteratorImpl(createConfig(),
                new WrappedIterator<Row>(List.<Row>of().iterator()));

        // When
        boolean equal = iteratorImpl.recordsEqual(r1, r2);
        iteratorImpl.close();

        // Then
        assertThat(equal).isFalse();
    }

}
