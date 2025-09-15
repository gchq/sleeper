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
import sleeper.core.properties.model.DataEngine;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.util.ObjectFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class IteratorFactoryTest {

    @Test
    public void shouldInitialiseIterator() throws IteratorCreationException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new LongType()))
                .build();
        SortedRowIterator ageOffIterator = new IteratorFactory(
                new ObjectFactory(IteratorFactoryTest.class.getClassLoader()))
                .getIterator(IteratorConfig.builder()
                        .iteratorClassName(AgeOffIterator.class.getName())
                        .iteratorConfigString("value,1000")
                        .build(), schema);

        List<Row> rows = List.of(
                new Row(Map.of("key", "test", "value", 10L)),
                new Row(Map.of("key", "test2", "value", 9999999999999999L)));
        CloseableIterator<Row> iterator = new WrappedIterator<>(rows.iterator());

        // When
        List<Row> filtered = new ArrayList<>();
        ageOffIterator.apply(iterator).forEachRemaining(filtered::add);

        // Then
        assertThat(filtered).containsExactly(new Row(Map.of("key", "test2", "value", 9999999999999999L)));
    }

    @Test
    public void shouldCreateAggregatingIterator() throws IteratorCreationException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new LongType()))
                .build();
        SortedRowIterator ageOffIterator = new IteratorFactory(
                new ObjectFactory(IteratorFactoryTest.class.getClassLoader()))
                .getIterator(IteratorConfig.builder()
                        .iteratorClassName(DataEngine.AGGREGATION_ITERATOR_NAME)
                        .iteratorConfigString(";ageoff=value,1000,")
                        .build(), schema);

        List<Row> rows = List.of(
                new Row(Map.of("key", "test", "value", 10L)),
                new Row(Map.of("key", "test2", "value", 9999999999999999L)));
        CloseableIterator<Row> iterator = new WrappedIterator<>(rows.iterator());

        // When
        List<Row> filtered = new ArrayList<>();
        ageOffIterator.apply(iterator).forEachRemaining(filtered::add);

        // Then
        assertThat(filtered).containsExactly(new Row(Map.of("key", "test2", "value", 9999999999999999L)));
    }

    @Test
    public void shouldUseFiltersDataOverClassNameWhenBothSet() throws IteratorCreationException {
        //When
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new LongType()),
                        new Field("otherValue", new LongType()))
                .build();
        SortedRowIterator iterator = new IteratorFactory(
                new ObjectFactory(IteratorFactoryTest.class.getClassLoader()))
                .getIterator(IteratorConfig.builder()
                        .iteratorClassName(DataEngine.AGGREGATION_ITERATOR_NAME)
                        .iteratorConfigString("someFakeConfig") //Would throw illegal argument exception if used
                        .filters("ageOff(otherValue,1000)")
                        .build(), schema);

        //Then
        assertThat(iterator.getRequiredValueFields()).containsExactly("key", "otherValue");
    }

    @Test
    public void shouldUseAggregationsDataOverClassNameWhenBothSet() throws IteratorCreationException {
        //When
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new LongType()),
                        new Field("otherValue", new LongType()))
                .build();
        SortedRowIterator iterator = new IteratorFactory(
                new ObjectFactory(IteratorFactoryTest.class.getClassLoader()))
                .getIterator(IteratorConfig.builder()
                        .iteratorClassName(DataEngine.AGGREGATION_ITERATOR_NAME)
                        .iteratorConfigString("someFakeConfig") //Would throw illegal argument exception if used
                        .aggregationString("sum(value),sum(otherValue)")
                        .build(), schema);

        //Then
        assertThat(iterator.getRequiredValueFields()).containsExactly("key", "value", "otherValue");
    }

    @Test
    public void shouldUseClassNameWhenNeitherFiltersOrAggregationsSet() throws IteratorCreationException {
        //When
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new LongType()),
                        new Field("otherValue", new LongType()))
                .build();
        SortedRowIterator iterator = new IteratorFactory(
                new ObjectFactory(IteratorFactoryTest.class.getClassLoader()))
                .getIterator(IteratorConfig.builder()
                        .iteratorClassName(DataEngine.AGGREGATION_ITERATOR_NAME)
                        .iteratorConfigString("value;ageOff=value,1000")
                        .filters(null)
                        .aggregationString(null)
                        .build(), schema);

        //Then
        assertThat(iterator.getRequiredValueFields()).containsExactly("key", "value");
    }

    @Test
    public void shouldThrowExceptionWhenUnknownFilterApplied() throws IteratorCreationException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new LongType()))
                .build();
        IteratorConfig config = IteratorConfig.builder()
                .filters("someother(value,1000)")
                .build();

        //Then
        assertThatThrownBy(() -> new IteratorFactory(
                new ObjectFactory(IteratorFactoryTest.class.getClassLoader()))
                .getIterator(config, schema))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Sleeper table filter not set to match ageOff(column,age), was: someother");
    }

    @Test
    public void shouldThrowExceptionWhenCantPassFilterValue() throws IteratorCreationException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new LongType()))
                .build();
        IteratorConfig config = IteratorConfig.builder()
                .filters("ageoff(value,oops)")
                .build();

        //Then
        assertThatThrownBy(() -> new IteratorFactory(
                new ObjectFactory(IteratorFactoryTest.class.getClassLoader()))
                .getIterator(config, schema))
                .isInstanceOf(NumberFormatException.class);
    }
}
