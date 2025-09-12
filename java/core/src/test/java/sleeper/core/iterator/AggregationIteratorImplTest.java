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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class AggregationIteratorImplTest {

    @Test
    public void shouldReturnFirstRowNoAggregations() throws IteratorCreationException {
        // Given
        Row r1 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "test", "value2", 78));
        Row r2 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "testaaaaa", "value2", 7800));

        Row expected = new Row(r1);

        // When
        AggregatorIteratorImpl.aggregateOnTo(r1, r2, new FilterAggregationConfig(List.of("TestColumn"),
                Optional.empty(),
                0L,
                List.of()));

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

        // When
        boolean equal = AggregatorIteratorImpl.rowsEqual(r1, r2, new FilterAggregationConfig(List.of("TestColumn"),
                Optional.empty(),
                0L,
                List.of()));

        // Then
        assertThat(equal).isTrue();
    }

    @Test
    public void shouldBeNonEqualRows() throws Exception {
        // Given
        Row r1 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 5, "value1", "test", "value2", 78));
        // Make sort_key2 different
        Row r2 = new Row(Map.of("key1", 12, "key2", "test", "sort_key", 9,
                "sort_key2", 6, "value1", "testaaaaa", "value2", 7800));

        // When
        boolean equal = AggregatorIteratorImpl.rowsEqual(r1, r2,
                new FilterAggregationConfig(List.of("key1", "key2", "sort_key", "sort_key2"),
                        Optional.empty(),
                        0L,
                        List.of()));

        // Then
        assertThat(equal).isFalse();
    }

}
