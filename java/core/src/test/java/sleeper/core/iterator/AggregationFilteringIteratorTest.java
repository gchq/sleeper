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
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

public class AggregationFilteringIteratorTest extends AggregationFilteringIteratorTestBase {
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

        SortedRowIterator iterator = createAggregationFilteringIteratorWithSchema(schema, "ageOff(value,1000)", null);

        // Then
        assertThat(iterator.getRequiredValueFields()).containsExactly("key", "value");
    }

    @ParameterizedTest
    @CsvSource({"ageoff", "AGEOFF", "ageOff"})
    public void shouldApplyAgeOffFilterFromProperties(String filters) throws IteratorCreationException {
        // Given
        SortedRowIterator ageOffIterator = createAggregationFilteringIterator(filters + "(value,1000)", null);

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
        List<Row> resultList = new ArrayList<>();
        sumAggregatorIterator.apply(iterator)
                .forEachRemaining(resultList::add);
        // Then
        assertThat(resultList).containsExactlyElementsOf(List.of(new Row(Map.of("key", "test", "value", 10142L))));
    }

    @ParameterizedTest
    @CsvSource({"map_sum", "Map_Sum", "MAP_SUM"})
    void shouldApplyMapSumAggregationFromProperties(String aggregator) throws IteratorCreationException {
        // Given
        SortedRowIterator sumAggregatorIterator = buildMapAggregator(aggregator);

        CloseableIterator<Row> iterator = new WrappedIterator<>(List.of(
                new Row(Map.of("key", "a", "sort", "b", "value", 1L, "map_value2",
                        Map.of("map_key1", 1L, "map_key2", 3L))),
                new Row(Map.of("key", "a", "sort", "b", "value", 2L, "map_value2",
                        Map.of("map_key1", 3L, "map_key2", 4L))))
                .iterator());

        // When
        List<Row> resultList = new ArrayList<>();
        sumAggregatorIterator.apply(iterator)
                .forEachRemaining(resultList::add);

        assertThat(resultList).containsExactly(
                new Row(Map.of("key", "a", "sort", "b", "value", 3L, "map_value2",
                        Map.of("map_key1", 4L, "map_key2", 7L))));

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
        List<Row> resultList = new ArrayList<>();
        minAggregatorIterator.apply(iterator)
                .forEachRemaining(resultList::add);

        // Then
        assertThat(resultList).containsExactlyElementsOf(List.of(new Row(Map.of("key", "test", "value", 97L))));
    }

    @ParameterizedTest
    @CsvSource({"map_min", "Map_Min", "MAP_MIN"})
    void shouldApplyMapMinAggregationFromProperties(String aggregator) throws IteratorCreationException {
        // Given
        SortedRowIterator minAggregatorIterator = buildMapAggregator(aggregator);

        CloseableIterator<Row> iterator = new WrappedIterator<>(List.of(
                new Row(Map.of("key", "a", "sort", "b", "value", 1L, "map_value2",
                        Map.of("map_key1", 17L, "map_key2", 112L))),
                new Row(Map.of("key", "a", "sort", "b", "value", 2L, "map_value2",
                        Map.of("map_key1", 9L, "map_key2", 2489L))))
                .iterator());

        // When
        List<Row> resultList = new ArrayList<>();
        minAggregatorIterator.apply(iterator)
                .forEachRemaining(resultList::add);

        assertThat(resultList).containsExactly(
                new Row(Map.of("key", "a", "sort", "b", "value", 3L, "map_value2",
                        Map.of("map_key1", 9L, "map_key2", 112L))));
    }

    @ParameterizedTest
    @CsvSource({"max", "Max", "MAX", "map_max", "Map_Max", "MAP_MAX"})
    public void shouldApplyMaxAggregationFromProperties(String aggregator) throws IteratorCreationException {
        // Given
        SortedRowIterator maxAggregatorIterator = buildSingleValueAggregator(aggregator);
        CloseableIterator<Row> iterator = new WrappedIterator<>(List.of(
                new Row(Map.of("key", "test", "value", 458498L)),
                new Row(Map.of("key", "test", "value", 87L)),
                new Row(Map.of("key", "test", "value", 222474L))).iterator());

        // When
        List<Row> resultList = new ArrayList<>();
        maxAggregatorIterator.apply(iterator)
                .forEachRemaining(resultList::add);

        // Then
        assertThat(resultList).containsExactlyElementsOf(List.of(new Row(Map.of("key", "test", "value", 458498L))));
    }

    @ParameterizedTest
    @CsvSource({"map_max", "Map_Max", "MAP_MAX"})
    void shouldApplyMapMaxAggregationFromProperties(String aggregator) throws IteratorCreationException {
        // Given
        SortedRowIterator maxAggregatorIterator = buildMapAggregator(aggregator);

        CloseableIterator<Row> iterator = new WrappedIterator<>(List.of(
                new Row(Map.of("key", "a", "sort", "b", "value", 1L, "map_value2",
                        Map.of("map_key1", 666L, "map_key2", 11L))),
                new Row(Map.of("key", "a", "sort", "b", "value", 2L, "map_value2",
                        Map.of("map_key1", 245L, "map_key2", 2L))))
                .iterator());

        // When
        List<Row> resultList = new ArrayList<>();
        maxAggregatorIterator.apply(iterator)
                .forEachRemaining(resultList::add);

        assertThat(resultList).containsExactly(
                new Row(Map.of("key", "a", "sort", "b", "value", 3L, "map_value2",
                        Map.of("map_key1", 666L, "map_key2", 11L))));
    }

    @Test
    void shouldApplyTwoAggregatorFromProperties() throws IteratorCreationException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(List.of(
                        new Field("key1", new StringType()),
                        new Field("key2", new StringType())))
                .valueFields(List.of(
                        new Field("value1", new LongType()),
                        new Field("value2", new LongType())))
                .build();

        SortedRowIterator doubleAggregatorIterator = createAggregationFilteringIteratorWithSchema(schema, null, "SUM(value1),MAX(value2)");

        CloseableIterator<Row> iterator = new WrappedIterator<>(List.of(
                new Row(Map.of("key1", "test", "value1", 4217L,
                        "key2", "test", "value2", 367L)),
                new Row(Map.of("key1", "test", "value1", 214L,
                        "key2", "test", "value2", 88818L)))
                .iterator());
        // When
        List<Row> resultList = new ArrayList<>();
        doubleAggregatorIterator.apply(iterator)
                .forEachRemaining(resultList::add);

        // Then
        assertThat(resultList.get(0).toString()).isEqualTo(
                new Row(Map.of("key1", "test", "value1", 4431,
                        "key2", "test", "value2", 88818L)).toString());
    }

    @Test
    void shouldThrowExceptionForInvalidOperandDeclared() throws IteratorCreationException {

        assertThatIllegalArgumentException().isThrownBy(() -> createAggregationFilteringIterator(null, "bop(VALUE)"))
                .withMessage("Unable to parse operand. Operand: bop");
    }

    @Test
    void shouldThrowExceptionWithKeyFieldIncludeAsAggregators() throws IteratorCreationException {
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("failKey", new StringType()))
                .sortKeyFields(new Field("sortKey", new StringType()))
                .valueFields(new Field("value", new LongType()))
                .build();

        assertThatIllegalArgumentException().isThrownBy(() -> createAggregationFilteringIteratorWithSchema(schema, null, "MIN(failKey),MIN(sortKey),SUM(value)"))
                .withMessage("Column for aggregation not allowed to be a Row Key or Sort Key. Column names: failKey, sortKey");
    }

    @Test
    void shouldThrowExceptionWhenDuplicateAggregators() {
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(new Field("doubleValue", new LongType()))
                .build();

        assertThatIllegalArgumentException().isThrownBy(() -> createAggregationFilteringIteratorWithSchema(schema, null, "MIN(doubleValue),SUM(doubleValue)"))
                .withMessage("Not allowed duplicate columns for aggregation. Column name: doubleValue");
    }

    @Test
    void shouldThrowExceptionWhenNotAllValueFieldsIncludedAsAggregator() {
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(new Field("existsValue", new LongType()), new Field("ignoredValue", new LongType()))
                .build();

        assertThatIllegalArgumentException().isThrownBy(() -> createAggregationFilteringIteratorWithSchema(schema, null, "MIN(existsValue)"))
                .withMessage("Not all value fields have aggregation declared. Missing columns: ignoredValue");

    }

    private SortedRowIterator buildSingleValueAggregator(String aggregator) throws IteratorCreationException {
        return createAggregationFilteringIterator(null, aggregator + "(value)");
    }

    private SortedRowIterator buildMapAggregator(String aggregator) throws IteratorCreationException {
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("sort", new StringType()))
                .valueFields(new Field("value", new LongType()), new Field("map_value2", new MapType(new StringType(), new LongType())))
                .build();

        String parsedAggregatorString = "sum(value)," + aggregator + "(map_value2)";
        return createAggregationFilteringIteratorWithSchema(schema, null, parsedAggregatorString);
    }
}
