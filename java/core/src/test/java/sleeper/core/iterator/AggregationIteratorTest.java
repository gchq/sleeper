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

import sleeper.core.iterator.testutil.SortedRowIteratorTestHelper;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class AggregationIteratorTest {

    /**
     * Note further coverage of this is in {@link AggregationFilteringIteratorTest}. This is to avoid coupling too much
     * to the structure of the aggregation iterator/configuration classes.
     */
    @Test
    void shouldAggregate() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(
                        new Field("groupBy", new StringType()),
                        new Field("aggregate", new LongType()))
                .build();
        List<Aggregation> aggregations = Aggregation.parseConfigWithAnyGrouping("sum(aggregate)", schema);
        List<String> groupByFields = List.of("key", "groupBy");

        // When
        SortedRowIterator iterator = new AggregationIterator(schema, groupByFields, aggregations);
        List<Row> foundRows = SortedRowIteratorTestHelper.apply(iterator, List.of(
                new Row(Map.of("key", "A", "groupBy", "a", "aggregate", 1L)),
                new Row(Map.of("key", "A", "groupBy", "a", "aggregate", 10L)),
                new Row(Map.of("key", "A", "groupBy", "b", "aggregate", 2L))));

        // Then
        assertThat(foundRows).containsExactly(
                new Row(Map.of("key", "A", "groupBy", "a", "aggregate", 11L)),
                new Row(Map.of("key", "A", "groupBy", "b", "aggregate", 2L)));
        assertThat(iterator.getRequiredValueFields()).containsExactly("groupBy");
    }

}
