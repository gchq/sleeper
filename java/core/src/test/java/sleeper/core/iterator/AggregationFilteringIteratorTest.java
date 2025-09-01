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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.util.IteratorConfig;
import sleeper.core.util.IteratorFactory;
import sleeper.core.util.IteratorFactoryTest;
import sleeper.core.util.ObjectFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

public class AggregationFilteringIteratorTest {
    @Test
    public void shouldThrowOnUninitialisedApply() {
        assertThatIllegalStateException().isThrownBy(() -> {
            // Given
            AggregationFilteringIterator afi = new AggregationFilteringIterator();
            afi.apply(null);
        }).withMessage("AggregatingIterator has not been initialised, call init()");
    }

    @Test
    public void shouldThrowOnUninitialisedGetRequiredValues() {
        assertThatIllegalStateException().isThrownBy(() -> {
            // Given
            AggregationFilteringIterator afi = new AggregationFilteringIterator();
            afi.getRequiredValueFields();
        }).withMessage("AggregatingIterator has not been initialised, call init()");
    }

    @Test
    public void shouldReturnValueFieldsWhenUsingFiltersProperty() throws IteratorCreationException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new LongType()),
                        new Field("notRequiredField", new IntType()))
                .build();
        SortedRowIterator iterator = new IteratorFactory(
                new ObjectFactory(IteratorFactoryTest.class.getClassLoader()))
                .getIterator(IteratorConfig.builder()
                        .filters("ageOff(value,1000)")
                        .build(), schema);
        // Then
        assertThat(iterator.getRequiredValueFields()).containsExactly("key", "value");
    }

    @ParameterizedTest
    @CsvSource({"ageoff", "AGEOFF", "ageOff"})
    public void shouldApplyAgeOffFilterFromProperties(String filters) throws IteratorCreationException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new LongType()))
                .build();
        SortedRowIterator ageOffIterator = new IteratorFactory(
                new ObjectFactory(IteratorFactoryTest.class.getClassLoader()))
                .getIterator(IteratorConfig.builder()
                        .filters(filters + "(value,1000)")
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

    @ParameterizedTest
    @CsvSource({"sum", "Sum", "SUM"})
    public void shouldApplySumAggregationFromProperties(String aggregator) throws IteratorCreationException {
        // Given
        SortedRowIterator sumAggregatorIterator = buildSingleValueAggregator(aggregator);
        CloseableIterator<Row> iterator = new WrappedIterator<>(List.of(
                new Row(Map.of("key", "test", "value", 2214L)),
                new Row(Map.of("key", "test", "value", 87L)),
                new Row(Map.of("key", "test", "value", 7841L))).iterator());

        // When
        Row resultant = sumAggregatorIterator.apply(iterator).next();

        // Then
        assertThat(resultant).isEqualTo(new Row(Map.of("key", "test", "value", 10142L)));
    }

    @ParameterizedTest
    @CsvSource({"min", "Min", "MIN"})
    public void shouldApplyMinAggregationFromProperties(String aggregator) throws IteratorCreationException {
        // Given
        SortedRowIterator minAggregatorIterator = buildSingleValueAggregator(aggregator);
        CloseableIterator<Row> iterator = new WrappedIterator<>(List.of(
                new Row(Map.of("key", "test", "value", 619L)),
                new Row(Map.of("key", "test", "value", 321L)),
                new Row(Map.of("key", "test", "value", 97L))).iterator());

        // When
        Row resultant = minAggregatorIterator.apply(iterator).next();

        // Then
        assertThat(resultant).isEqualTo(new Row(Map.of("key", "test", "value", 97L)));
    }

    @ParameterizedTest
    @CsvSource({"max", "Max", "MAX"})
    public void shouldApplyMaxAggregationFromProperties(String aggregator) throws IteratorCreationException {
        // Given
        SortedRowIterator maxAggregatorIterator = buildSingleValueAggregator(aggregator);
        CloseableIterator<Row> iterator = new WrappedIterator<>(List.of(
                new Row(Map.of("key", "test", "value", 458498L)),
                new Row(Map.of("key", "test", "value", 87L)),
                new Row(Map.of("key", "test", "value", 222474L))).iterator());

        // When
        Row resultant = maxAggregatorIterator.apply(iterator).next();

        // Then
        assertThat(resultant).isEqualTo(new Row(Map.of("key", "test", "value", 458498L)));
    }

    private SortedRowIterator buildSingleValueAggregator(String aggregator) throws IteratorCreationException {
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(new Field("value", new LongType()))
                .build();

        return new IteratorFactory(
                new ObjectFactory(IteratorFactoryTest.class.getClassLoader()))
                .getIterator(IteratorConfig.builder()
                        .filters("")
                        .aggregationString(aggregator + "(value)")
                        .build(), schema);
    }
}
