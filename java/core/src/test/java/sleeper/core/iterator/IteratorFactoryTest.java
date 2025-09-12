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

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.AGGREGATION_CONFIG;
import static sleeper.core.properties.table.TableProperty.FILTERING_CONFIG;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;

public class IteratorFactoryTest {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);

    @Test
    public void shouldInitialiseIterator() throws Exception {
        // Given
        tableProperties.setSchema(Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new LongType()))
                .build());
        tableProperties.set(ITERATOR_CLASS_NAME, AgeOffIterator.class.getName());
        tableProperties.set(ITERATOR_CONFIG, "value,1000");

        // When
        List<Row> filtered = applyIterator(List.of(
                new Row(Map.of("key", "test", "value", 10L)),
                new Row(Map.of("key", "test2", "value", 9999999999999999L))));

        // Then
        assertThat(filtered).containsExactly(
                new Row(Map.of("key", "test2", "value", 9999999999999999L)));
    }

    @Test
    public void shouldCreateAggregatingIterator() throws Exception {
        // Given
        tableProperties.setSchema(Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new LongType()))
                .build());
        tableProperties.set(ITERATOR_CLASS_NAME, DataEngine.AGGREGATION_ITERATOR_NAME);
        tableProperties.set(ITERATOR_CONFIG, ";ageoff=value,1000,");

        // When
        List<Row> filtered = applyIterator(List.of(
                new Row(Map.of("key", "test", "value", 10L)),
                new Row(Map.of("key", "test2", "value", 9999999999999999L))));

        // Then
        assertThat(filtered).containsExactly(
                new Row(Map.of("key", "test2", "value", 9999999999999999L)));
    }

    @Test
    public void shouldUseFilteringConfigOverClassNameWhenBothSet() throws Exception {
        // Given
        tableProperties.setSchema(Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new LongType()),
                        new Field("otherValue", new LongType()))
                .build());
        tableProperties.set(ITERATOR_CLASS_NAME, DataEngine.AGGREGATION_ITERATOR_NAME);
        tableProperties.set(ITERATOR_CONFIG, "someFakeConfig"); //Would throw illegal argument exception if used
        tableProperties.set(FILTERING_CONFIG, "ageOff(value,1000)");

        // When
        List<Row> filtered = applyIterator(List.of(
                new Row(Map.of("key", "test", "value", 10L)),
                new Row(Map.of("key", "test2", "value", 9999999999999999L))));

        // Then
        assertThat(filtered).containsExactly(
                new Row(Map.of("key", "test2", "value", 9999999999999999L)));
    }

    @Test
    public void shouldUseAggregationConfigOverClassNameWhenBothSet() throws Exception {
        // Given
        tableProperties.setSchema(Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new LongType()),
                        new Field("otherValue", new LongType()))
                .build());
        tableProperties.set(ITERATOR_CLASS_NAME, DataEngine.AGGREGATION_ITERATOR_NAME);
        tableProperties.set(ITERATOR_CONFIG, "someFakeConfig"); //Would throw illegal argument exception if used
        tableProperties.set(AGGREGATION_CONFIG, "sum(value),sum(otherValue)");

        // When
        List<Row> filtered = applyIterator(List.of(
                new Row(Map.of("key", "test", "value", 100L, "otherValue", 200L)),
                new Row(Map.of("key", "test", "value", 1000L, "otherValue", 2000L))));

        // Then
        assertThat(filtered).containsExactly(
                new Row(Map.of("key", "test", "value", 1100L, "otherValue", 2200L)));
    }

    @Test
    public void shouldThrowExceptionWhenUnknownFilterApplied() {
        // Given
        tableProperties.setSchema(Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new LongType()))
                .build());
        tableProperties.set(FILTERING_CONFIG, "someother(value,1000)");

        // When / Then
        assertThatThrownBy(() -> createIterator())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Sleeper table filter not set to match ageOff(column,age), was: someother");
    }

    @Test
    public void shouldThrowExceptionWhenCantPassFilterValue() {
        // Given
        tableProperties.setSchema(Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new LongType()))
                .build());
        tableProperties.set(FILTERING_CONFIG, "ageOff(value,oops)");

        // When / Then
        assertThatThrownBy(() -> createIterator())
                .isInstanceOf(NumberFormatException.class);
    }

    private List<Row> applyIterator(List<Row> rows) throws Exception {
        return SortedRowIteratorTestHelper.apply(createIterator(), rows);
    }

    private SortedRowIterator createIterator() throws Exception {
        return IteratorFactoryTestHelper.createIterator(tableProperties);
    }
}
