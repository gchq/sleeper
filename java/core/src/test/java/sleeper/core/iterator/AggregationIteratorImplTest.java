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

import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static sleeper.core.iterator.AggregatorIteratorImpl.aggregateOnTo;

public class AggregationIteratorImplTest extends AggregationFilteringIteratorTestBase {

    @Test
    public void shouldThrowOnNullIterator() {
        assertThatNullPointerException().isThrownBy(() -> new AggregatorIteratorImpl(createAggregationFilteringIterator(null, null, null).getFilterAggregationConfig(), null));
    }

    @Test
    public void shouldThrowOnNullConfig() {
        assertThatNullPointerException().isThrownBy(() -> new AggregatorIteratorImpl(null,
                new WrappedIterator<Row>(List.<Row>of().iterator())));
    }

    @Test
    public void shouldAggregate() throws IteratorCreationException {
        // Given
        Row r1 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "test", "value2", 78));
        Row r2 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "testaaaaa", "value2", 7800));

        Row expected = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "testtestaaaaa", "value2", 78));
        // When
        aggregateOnTo(r1, r2, createAggregationFilteringIterator(null, null, "sum(value1),min(value2)").getFilterAggregationConfig());

        // Then
        assertThat(r1).isEqualTo(expected);
    }

    @Test
    public void shouldReturnFirstRowNoAggregations() throws IteratorCreationException {
        // Given
        Row r1 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "test", "value2", 78));
        Row r2 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "testaaaaa", "value2", 7800));

        Row expected = new Row(r1);
        // When
        aggregateOnTo(r1, r2, createAggregationFilteringIterator(null, "", "").getFilterAggregationConfig());

        // Then
        assertThat(r1).isEqualTo(expected);
    }

    @Test
    public void shouldBeEqualRows() throws Exception {

        // Given
        Row r1 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "test", "value2", 78));
        Row r2 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "testaaaaa", "value2", 7800));
        AggregatorIteratorImpl iteratorImpl = new AggregatorIteratorImpl(createAggregationFilteringIterator(null, "", "").getFilterAggregationConfig(),
                new WrappedIterator<Row>(List.<Row>of().iterator()));

        // When
        boolean equal = iteratorImpl.rowsEqual(r1, r2);
        iteratorImpl.close();

        // Then
        assertThat(equal).isTrue();
    }

    @Test
    public void shouldBeNonEqualRows() throws Exception {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key1", new IntType()), new Field("key2", new IntType()))
                .sortKeyFields(new Field("sort_key", new StringType()), new Field("sort_key2", new StringType()))
                .valueFields(new Field("value", new LongType()))
                .build();
        Row r1 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "test", "value2", 78));
        // Make sort_key2 different
        Row r2 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 6, "value1", "testaaaaa", "value2", 7800));

        AggregatorIteratorImpl iteratorImpl = new AggregatorIteratorImpl(createAggregationFilteringIterator(schema, "", "").getFilterAggregationConfig(),
                new WrappedIterator<Row>(List.<Row>of().iterator()));

        // When
        boolean equal = iteratorImpl.rowsEqual(r1, r2);
        iteratorImpl.close();

        // Then
        assertThat(equal).isFalse();
    }

}
