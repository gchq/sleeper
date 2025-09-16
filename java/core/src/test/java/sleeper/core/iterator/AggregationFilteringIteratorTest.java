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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import sleeper.core.iterator.testutil.IteratorFactoryTestHelper;
import sleeper.core.iterator.testutil.SortedRowIteratorTestHelper;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.AGGREGATION_CONFIG;
import static sleeper.core.properties.table.TableProperty.FILTERING_CONFIG;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;

public class AggregationFilteringIteratorTest {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties,
            Schema.builder()
                    .rowKeyFields(new Field("key", new StringType()))
                    .valueFields(new Field("value", new LongType()))
                    .build());

    @Nested
    @DisplayName("Apply filters")
    class ApplyFilters {

        @ParameterizedTest
        @CsvSource({"ageoff", "AGEOFF", "ageOff"})
        public void shouldApplyAgeOffFilterFromProperties(String ageOff) throws Exception {
            // Given
            tableProperties.set(FILTERING_CONFIG, ageOff + "(value,1000)");

            List<Row> rows = List.of(
                    new Row(Map.of("key", "test", "value", 10L)),
                    new Row(Map.of("key", "test2", "value", 9999999999999999L)));

            // When
            List<Row> filtered = applyIterator(rows);

            // Then
            assertThat(filtered).containsExactly(new Row(Map.of("key", "test2", "value", 9999999999999999L)));
        }
    }

    @Nested
    @DisplayName("Apply aggregations against single fields")
    class ApplySingleFieldAggregations {

        @ParameterizedTest
        @CsvSource({"sum", "Sum", "SUM"})
        public void shouldApplySumAggregationFromProperties(String aggregator) throws Exception {
            // Given
            tableProperties.set(AGGREGATION_CONFIG, aggregator + "(value)");

            // When
            List<Row> resultList = applyIterator(List.of(
                    new Row(Map.of("key", "test", "value", 2214L)),
                    new Row(Map.of("key", "test", "value", 87L)),
                    new Row(Map.of("key", "test", "value", 7841L))));

            // Then
            assertThat(resultList).containsExactlyElementsOf(List.of(new Row(Map.of("key", "test", "value", 10142L))));
        }

        @ParameterizedTest
        @CsvSource({"min", "Min", "MIN"})
        public void shouldApplyMinAggregationFromProperties(String aggregator) throws Exception {
            // Given
            tableProperties.set(AGGREGATION_CONFIG, aggregator + "(value)");

            // When
            List<Row> resultList = applyIterator(List.of(
                    new Row(Map.of("key", "test", "value", 619L)),
                    new Row(Map.of("key", "test", "value", 321L)),
                    new Row(Map.of("key", "test", "value", 97L))));

            // Then
            assertThat(resultList).containsExactlyElementsOf(List.of(new Row(Map.of("key", "test", "value", 97L))));
        }

        @ParameterizedTest
        @CsvSource({"max", "Max", "MAX"})
        public void shouldApplyMaxAggregationFromProperties(String aggregator) throws Exception {
            // Given
            tableProperties.set(AGGREGATION_CONFIG, aggregator + "(value)");

            // When
            List<Row> resultList = applyIterator(List.of(
                    new Row(Map.of("key", "test", "value", 458498L)),
                    new Row(Map.of("key", "test", "value", 87L)),
                    new Row(Map.of("key", "test", "value", 222474L))));

            // Then
            assertThat(resultList).containsExactlyElementsOf(List.of(new Row(Map.of("key", "test", "value", 458498L))));
        }
    }

    @Nested
    @DisplayName("Apply aggregations against map fields")
    class ApplyMapAggregations {

        @BeforeEach
        void setUp() {
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("key", new StringType()))
                    .valueFields(new Field("map_value", new MapType(new StringType(), new LongType())))
                    .build());
        }

        @ParameterizedTest
        @CsvSource({"map_sum", "Map_Sum", "MAP_SUM"})
        void shouldApplyMapSumAggregationFromProperties(String aggregator) throws Exception {
            // Given
            tableProperties.set(AGGREGATION_CONFIG, aggregator + "(map_value)");

            // When
            List<Row> resultList = applyIterator(List.of(
                    new Row(Map.of("key", "a",
                            "map_value", Map.of("map_key1", 1L, "map_key2", 3L))),
                    new Row(Map.of("key", "a",
                            "map_value", Map.of("map_key1", 3L, "map_key2", 4L)))));

            assertThat(resultList).containsExactly(
                    new Row(Map.of("key", "a",
                            "map_value", Map.of("map_key1", 4L, "map_key2", 7L))));
        }

        @ParameterizedTest
        @CsvSource({"map_min", "Map_Min", "MAP_MIN"})
        void shouldApplyMapMinAggregationFromProperties(String aggregator) throws Exception {
            // Given
            tableProperties.set(AGGREGATION_CONFIG, aggregator + "(map_value)");

            // When
            List<Row> resultList = applyIterator(List.of(
                    new Row(Map.of("key", "a",
                            "map_value", Map.of("map_key1", 17L, "map_key2", 112L))),
                    new Row(Map.of("key", "a",
                            "map_value", Map.of("map_key1", 9L, "map_key2", 2489L)))));

            assertThat(resultList).containsExactly(
                    new Row(Map.of("key", "a",
                            "map_value", Map.of("map_key1", 9L, "map_key2", 112L))));
        }

        @ParameterizedTest
        @CsvSource({"map_max", "Map_Max", "MAP_MAX"})
        void shouldApplyMapMaxAggregationFromProperties(String aggregator) throws Exception {
            // Given
            tableProperties.set(AGGREGATION_CONFIG, aggregator + "(map_value)");

            // When
            List<Row> resultList = applyIterator(List.of(
                    new Row(Map.of("key", "a",
                            "map_value", Map.of("map_key1", 666L, "map_key2", 11L))),
                    new Row(Map.of("key", "a",
                            "map_value", Map.of("map_key1", 245L, "map_key2", 2L)))));

            assertThat(resultList).containsExactly(
                    new Row(Map.of("key", "a",
                            "map_value", Map.of("map_key1", 666L, "map_key2", 11L))));
        }
    }

    @Nested
    @DisplayName("Combine configuration together")
    class CombineConfiguration {

        @Test
        void shouldApplyTwoAggregatorFromProperties() throws Exception {
            // Given
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(List.of(
                            new Field("key1", new StringType()),
                            new Field("key2", new StringType())))
                    .valueFields(List.of(
                            new Field("value1", new LongType()),
                            new Field("value2", new LongType())))
                    .build());

            tableProperties.set(AGGREGATION_CONFIG, "SUM(value1),MAX(value2)");

            // When
            List<Row> resultList = applyIterator(List.of(
                    new Row(Map.of("key1", "test", "value1", 4217L,
                            "key2", "test", "value2", 367L)),
                    new Row(Map.of("key1", "test", "value1", 214L,
                            "key2", "test", "value2", 88818L))));

            // Then
            assertThat(resultList).containsExactly(
                    new Row(Map.of("key1", "test", "value1", 4431L,
                            "key2", "test", "value2", 88818L)));
        }

        @Test
        void shouldApplyAggregationAndFiltering() throws Exception {
            // Given
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("key", new StringType()))
                    .valueFields(
                            new Field("timestamp", new LongType()),
                            new Field("count", new LongType()))
                    .build());
            tableProperties.set(AGGREGATION_CONFIG, "MAX(timestamp),SUM(count)");
            tableProperties.set(FILTERING_CONFIG, "ageOff(timestamp,1000)");

            // When
            List<Row> resultList = applyIterator(List.of(
                    new Row(Map.of("key", "A", "timestamp", 9999999999999999L, "count", 10L)),
                    new Row(Map.of("key", "A", "timestamp", 100L, "count", 20L)), // Filtered out due to age
                    new Row(Map.of("key", "B", "timestamp", 100L, "count", 10L)), // Filtered out due to age
                    new Row(Map.of("key", "C", "timestamp", 100L, "count", 10L)), // Filtered out due to age
                    new Row(Map.of("key", "C", "timestamp", 100L, "count", 20L)), // Filtered out due to age
                    new Row(Map.of("key", "D", "timestamp", 9999999999999999L, "count", 10L)),
                    new Row(Map.of("key", "D", "timestamp", 9999999999999999L, "count", 20L))));

            // Then
            assertThat(resultList).containsExactly(
                    new Row(Map.of("key", "A", "timestamp", 9999999999999999L, "count", 10L)),
                    new Row(Map.of("key", "D", "timestamp", 9999999999999999L, "count", 30L)));
        }
    }

    @Nested
    @DisplayName("Process rows from iterator")
    class ProcessRows {

        @BeforeEach
        void setUp() {
            tableProperties.set(AGGREGATION_CONFIG, "sum(value)");
        }

        @Test
        void shouldAggregateZeroRows() throws Exception {
            // When / Then
            assertThat(applyIterator(List.of())).isEmpty();
        }

        @Test
        void shouldAggregateOneRow() throws Exception {
            // Given
            Row testRow = new Row(Map.of("key", "A", "value", 525L));

            // When
            List<Row> resultList = applyIterator(List.of(testRow));

            // Then
            assertThat(resultList).containsExactly(testRow);
        }

        @Test
        void shouldAggregateTwoDifferentKeyRows() throws Exception {
            // Given
            List<Row> testRows = List.of(
                    new Row(Map.of("key", "A", "value", 100L)),
                    new Row(Map.of("key", "B", "value", 1000L)));

            // When
            List<Row> resultList = applyIterator(testRows);

            // Then
            assertThat(resultList).isEqualTo(testRows);
        }

        @Test
        void shouldAggregateTwoEqualThenOneDifferentRow() throws Exception {
            // When
            List<Row> resultList = applyIterator(List.of(
                    new Row(Map.of("key", "A", "value", 100L)),
                    new Row(Map.of("key", "A", "value", 1000L)),
                    new Row(Map.of("key", "B", "value", 500L))));

            // Then
            assertThat(resultList).isEqualTo(List.of(
                    new Row(Map.of("key", "A", "value", 1100L)),
                    new Row(Map.of("key", "B", "value", 500L))));
        }

        @Test
        void shouldAggregateOneDifferentThenTwoEqualRow() throws Exception {
            // When
            List<Row> resultList = applyIterator(List.of(
                    new Row(Map.of("key", "A", "value", 100L)),
                    new Row(Map.of("key", "B", "value", 1000L)),
                    new Row(Map.of("key", "B", "value", 500L))));

            // Then
            assertThat(resultList).isEqualTo(List.of(
                    new Row(Map.of("key", "A", "value", 100L)),
                    new Row(Map.of("key", "B", "value", 1500L))));
        }

        @Test
        void shouldAggregateTwoSameThenOneDifferentThenTwoEqualRow() throws Exception {
            // When
            List<Row> resultList = applyIterator(List.of(
                    new Row(Map.of("key", "A", "value", 100L)),
                    new Row(Map.of("key", "A", "value", 1000L)),
                    new Row(Map.of("key", "B", "value", 500L)),
                    new Row(Map.of("key", "C", "value", 70L)),
                    new Row(Map.of("key", "C", "value", 230L))));

            // Then
            assertThat(resultList).isEqualTo(List.of(
                    new Row(Map.of("key", "A", "value", 1100L)),
                    new Row(Map.of("key", "B", "value", 500L)),
                    new Row(Map.of("key", "C", "value", 300L))));
        }

        @Test
        void shouldAggregateOneDifferentThenTwoSameThenOneDifferent() throws Exception {
            // When
            List<Row> resultList = applyIterator(List.of(
                    new Row(Map.of("key", "A", "value", 100L)),
                    new Row(Map.of("key", "B", "value", 1000L)),
                    new Row(Map.of("key", "B", "value", 500L)),
                    new Row(Map.of("key", "C", "value", 70L))));

            // Then
            assertThat(resultList).isEqualTo(List.of(
                    new Row(Map.of("key", "A", "value", 100L)),
                    new Row(Map.of("key", "B", "value", 1500L)),
                    new Row(Map.of("key", "C", "value", 70L))));
        }

        @Test
        void shouldAggregateRowsWhereRowAndSortKeyAreEqual() throws Exception {
            // Given
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("key", new StringType()))
                    .sortKeyFields(new Field("sortKey", new IntType()))
                    .valueFields(new Field("value", new IntType()))
                    .build());
            tableProperties.set(AGGREGATION_CONFIG, "sum(value)");

            // When
            List<Row> resultList = applyIterator(List.of(
                    new Row(Map.of("key", "A", "sortKey", 1, "value", 100L)),
                    new Row(Map.of("key", "A", "sortKey", 1, "value", 1000L)),
                    new Row(Map.of("key", "A", "sortKey", 2, "value", 500L))));

            // Then
            assertThat(resultList).containsExactly(
                    new Row(Map.of("key", "A", "sortKey", 1, "value", 1100L)),
                    new Row(Map.of("key", "A", "sortKey", 2, "value", 500L)));
        }
    }

    @Nested
    @DisplayName("Report required value fields")
    class RequiredValueFields {

        @Test
        public void shouldReturnValueFieldsWhenUsingFiltersProperty() throws Exception {
            // Given
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("key", new IntType()))
                    .valueFields(new Field("value", new LongType()),
                            new Field("notRequiredField", new IntType()))
                    .build());
            tableProperties.set(FILTERING_CONFIG, "ageOff(value,1000)");

            // When / Then
            assertThat(createIterator().getRequiredValueFields()).containsExactly("value");
        }

        @Test
        void shouldSetRequiredValueFieldsWhenFilteringOnSortKey() throws Exception {
            // Given
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("key", new StringType()))
                    .sortKeyFields(new Field("sortKey", new LongType()))
                    .valueFields(new Field("value", new IntType()))
                    .build());
            tableProperties.set(FILTERING_CONFIG, "ageOff(sortKey, 1000)");

            // When / Then
            // TODO this is incorrect, there should be no required value fields
            // See https://github.com/gchq/sleeper/issues/5605
            assertThat(createIterator().getRequiredValueFields()).containsExactly("sortKey");
        }

        @Test
        void shouldSetRequiredValueFieldsWhenAggregating() throws Exception {
            // Given
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("key", new StringType()))
                    .sortKeyFields(new Field("sortKey", new LongType()))
                    .valueFields(
                            new Field("value1", new IntType()),
                            new Field("value2", new IntType()),
                            new Field("value3", new IntType()))
                    .build());
            tableProperties.set(AGGREGATION_CONFIG, "sum(value1), min(value2), max(value3)");

            // When / Then
            // TODO this is incorrect, there should be no required value fields
            // See https://github.com/gchq/sleeper/issues/5605
            assertThat(createIterator().getRequiredValueFields()).containsExactly("key", "sortKey", "value1", "value2", "value3");
        }
    }

    @Nested
    @DisplayName("Validate filter configuration")
    class ValidateFilterConfiguration {
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
                    .isInstanceOf(IteratorCreationException.class)
                    .cause()
                    .isInstanceOf(IllegalArgumentException.class)
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
                    .isInstanceOf(IteratorCreationException.class)
                    .hasCauseInstanceOf(NumberFormatException.class);
        }
    }

    @Nested
    @DisplayName("Validate aggregation configuration")
    class ValidateAggregationConfiguration {

        @Test
        void shouldThrowExceptionForInvalidOperandDeclared() throws IteratorCreationException {
            // Given
            tableProperties.set(AGGREGATION_CONFIG, "bop(VALUE)");

            // When / Then
            assertThatThrownBy(() -> createIterator())
                    .isInstanceOf(IteratorCreationException.class)
                    .cause()
                    .hasMessage("Unable to parse operand. Operand: bop");
        }

        @Test
        void shouldThrowExceptionWhenAggregatingNonExistentField() throws IteratorCreationException {
            // Given
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("key", new StringType()))
                    .valueFields(new Field("value", new LongType()))
                    .build());
            tableProperties.set(AGGREGATION_CONFIG, "SUM(value),SUM(abc)");

            // When / Then
            assertThatThrownBy(() -> createIterator())
                    .isInstanceOf(IteratorCreationException.class)
                    .cause()
                    .hasMessage("Not all aggregated fields are declared in the schema. Missing fields: abc");
        }

        @Test
        void shouldThrowExceptionWithKeyFieldIncludeAsAggregators() throws IteratorCreationException {
            // Given
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("failKey", new StringType()))
                    .sortKeyFields(new Field("sortKey", new StringType()))
                    .valueFields(new Field("value", new LongType()))
                    .build());
            tableProperties.set(AGGREGATION_CONFIG, "MIN(failKey),MIN(sortKey),SUM(value)");

            // When / Then
            assertThatThrownBy(() -> createIterator())
                    .isInstanceOf(IteratorCreationException.class)
                    .cause()
                    .hasMessage("Column for aggregation not allowed to be a Row Key or Sort Key. Column names: failKey, sortKey");
        }

        @Test
        void shouldThrowExceptionWhenDuplicateAggregators() {
            // Given
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("key", new StringType()))
                    .valueFields(new Field("doubleValue", new LongType()))
                    .build());
            tableProperties.set(AGGREGATION_CONFIG, "MIN(doubleValue),SUM(doubleValue)");

            // When / Then
            assertThatThrownBy(() -> createIterator())
                    .isInstanceOf(IteratorCreationException.class)
                    .cause()
                    .hasMessage("Not allowed duplicate columns for aggregation. Column name: doubleValue");
        }

        @Test
        void shouldThrowExceptionWhenNotAllValueFieldsIncludedAsAggregator() {
            // Given
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("key", new StringType()))
                    .valueFields(new Field("existsValue", new LongType()), new Field("ignoredValue", new LongType()))
                    .build());
            tableProperties.set(AGGREGATION_CONFIG, "MIN(existsValue)");

            // When / Then
            assertThatThrownBy(() -> createIterator())
                    .isInstanceOf(IteratorCreationException.class)
                    .cause()
                    .hasMessage("Not all value fields have aggregation declared. Missing columns: ignoredValue");
        }
    }

    @Nested
    @DisplayName("Config format flexibility")
    class ConfigFormat {

        @Test
        void shouldRemoveWhitespaceFromFilterConfigurationAndApply() throws Exception {
            // Given
            tableProperties.set(FILTERING_CONFIG, " ageOff ( value , 1000 ) ");

            List<Row> rows = List.of(
                    new Row(Map.of("key", "test", "value", 10L)),
                    new Row(Map.of("key", "test2", "value", 9999999999999999L)));

            // When
            List<Row> filtered = applyIterator(rows);

            // Then
            assertThat(filtered).containsExactly(new Row(Map.of("key", "test2", "value", 9999999999999999L)));
        }

        @Test
        void shouldRemoveWhitespaceFromAggregatorConfigurationAndApply() throws Exception {
            // Given
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("key", new StringType()))
                    .valueFields(new Field("value", new LongType()), new Field("value2", new LongType()))
                    .build());
            tableProperties.set(AGGREGATION_CONFIG, " min ( value ) , sum ( value2 )");

            // When
            List<Row> resultList = applyIterator(List.of(
                    new Row(Map.of("key", "A", "value", 100L, "value2", 10L)),
                    new Row(Map.of("key", "A", "value", 1000L, "value2", 20L))));

            // Then
            assertThat(resultList).isEqualTo(List.of(
                    new Row(Map.of("key", "A", "value", 100L, "value2", 30L))));
        }
    }

    private List<Row> applyIterator(List<Row> rows) throws Exception {
        return SortedRowIteratorTestHelper.apply(createIterator(), rows);
    }

    private SortedRowIterator createIterator() throws Exception {
        return IteratorFactoryTestHelper.createIterator(tableProperties);
    }
}
