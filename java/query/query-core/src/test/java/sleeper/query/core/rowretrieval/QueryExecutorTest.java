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
package sleeper.query.core.rowretrieval;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.example.iterator.AdditionIterator;
import sleeper.example.iterator.SecurityFilteringIterator;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryException;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.AGGREGATION_CONFIG;
import static sleeper.core.properties.table.TableProperty.FILTERING_CONFIG;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.core.properties.table.TableProperty.QUERY_PROCESSOR_CACHE_TIMEOUT;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class QueryExecutorTest extends QueryExecutorTestBase {

    @Nested
    @DisplayName("Query rows")
    class QueryRows {

        @Test
        void shouldReturnSubRangeInSinglePartition() throws Exception {
            // Given
            addRootFile("file.parquet", List.of(new Row(Map.of("key", 123L))));

            // When
            List<Row> rows = getRows(queryRange(100L, 200L));

            // Then
            assertThat(rows).containsExactly(new Row(Map.of("key", 123L)));
        }

        @Test
        void shouldReturnRowInOneOfTwoRanges() throws Exception {
            // Given
            addRootFile("file.parquet", List.of(new Row(Map.of("key", 123L))));

            // When
            List<Row> rows = getRows(queryRegions(range(100L, 200L), range(400L, 500L)));

            // Then
            assertThat(rows).containsExactly(new Row(Map.of("key", 123L)));
        }

        @Test
        void shouldNotFindRowOutsideSubRangeInSinglePartition() throws Exception {
            // Given
            addRootFile("file.parquet", List.of(new Row(Map.of("key", 123L))));

            // When
            List<Row> rows = getRows(queryRange(200L, 300L));

            // Then
            assertThat(rows).isEmpty();
        }

        @Test
        void shouldNotFindRowOutsidePartitionRangeWhenFileContainsAnInactiveRow() throws Exception {
            // Given
            update(stateStore).initialise(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 5L)
                    .buildList());
            addPartitionFile("R", "file.parquet", List.of(
                    new Row(Map.of("key", 2L)),
                    new Row(Map.of("key", 7L))));

            // When
            List<Row> rows = getRows(queryAllRows());

            // Then
            assertThat(rows).containsExactly(
                    new Row(Map.of("key", 7L)));
        }

        @Test
        void shouldFailIfAFileDoesNotExist() {
            addFileMetadata(fileReferenceFactory().rootFile("file.parquet", 10L));

            // When / Then
            assertThatThrownBy(() -> getRows(queryAllRows()))
                    .isInstanceOf(RuntimeException.class)
                    .cause().isInstanceOf(QueryException.class)
                    .cause().isInstanceOf(RowRetrievalException.class)
                    .cause().isInstanceOf(NoSuchElementException.class)
                    .hasNoCause();
        }
    }

    @Nested
    @DisplayName("Request value fields")
    class RequestValueFields {

        @BeforeEach
        void setUp() throws Exception {
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("key", new LongType()))
                    .valueFields(
                            new Field("A", new StringType()),
                            new Field("B", new LongType()),
                            new Field("C", new ByteArrayType()))
                    .build());
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
        }

        @Test
        void shouldReturnAllFieldsWhenNotRequestingValueFields() throws Exception {
            // Given
            addRootFile("file.parquet", List.of(
                    new Row(Map.of(
                            "key", 1L,
                            "A", "first",
                            "B", 11L,
                            "C", new byte[]{1, 1}))));

            // When
            List<Row> rows = getRows(queryAllRows());

            // Then
            assertThat(rows).containsExactly(
                    new Row(Map.of(
                            "key", 1L,
                            "A", "first",
                            "B", 11L,
                            "C", new byte[]{1, 1})));
        }

        @Test
        void shouldExcludeFieldsWhenRequestingValueFields() throws Exception {
            // Given
            addRootFile("file.parquet", List.of(
                    new Row(Map.of(
                            "key", 1L,
                            "A", "first",
                            "B", 11L,
                            "C", new byte[]{1, 1}))));

            // When
            List<Row> rows = getRows(queryAllRowsBuilder()
                    .processingConfig(requestValueFields("A"))
                    .build());

            // Then
            assertThat(rows).containsExactly(
                    new Row(Map.of("key", 1L, "A", "first")));
        }
    }

    @Nested
    @DisplayName("Request value fields with aggregation and filtering enabled")
    class RequestValueFieldsWithAggregationAndFiltering {

        @BeforeEach
        void setUp() {
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("key", new LongType()))
                    .sortKeyFields(new Field("sort", new LongType()))
                    .valueFields(
                            new Field("A", new LongType()),
                            new Field("B", new LongType()),
                            new Field("C", new LongType()))
                    .build());
        }

        Query queryOnlyFieldA = queryAllRowsBuilder()
                .processingConfig(requestValueFields("A"))
                .build();

        @Test
        void shouldNotReadAnyExtraFieldsWhenApplyingAggregation() throws Exception {
            // Given
            tableProperties.set(AGGREGATION_CONFIG, "sum(A),min(B),max(C)");
            addRootFile("file.parquet", List.of(
                    new Row(Map.of(
                            "key", 1L,
                            "sort", 10L,
                            "A", 100L,
                            "B", 1000L,
                            "C", 10000L))));

            // When
            List<Row> rows = getRows(queryOnlyFieldA);

            // Then
            assertThat(rows).containsExactly(
                    new Row(Map.of("key", 1L, "sort", 10L, "A", 100L)));
        }

        @Test
        void shouldFilterOnRequestedValueField() throws Exception {
            // Given
            tableProperties.set(FILTERING_CONFIG, "ageOff(A, 1000)");
            addRootFile("file.parquet", List.of(
                    new Row(Map.of(
                            "key", 1L,
                            "sort", 10L,
                            "A", 9999999999999999L,
                            "B", 1000L,
                            "C", 10000L))));

            // When
            List<Row> rows = getRows(queryOnlyFieldA);

            // Then
            assertThat(rows).containsExactly(
                    new Row(Map.of(
                            "key", 1L,
                            "sort", 10L,
                            "A", 9999999999999999L)));
        }

        @Test
        void shouldReadExtraFieldWhenFilteringOnAFieldThatWasNotRequested() throws Exception {
            // Given
            tableProperties.set(FILTERING_CONFIG, "ageOff(B, 1000)");
            addRootFile("file.parquet", List.of(
                    new Row(Map.of(
                            "key", 1L,
                            "sort", 10L,
                            "A", 100L,
                            "B", 9999999999999999L,
                            "C", 10000L))));

            // When
            List<Row> rows = getRows(queryOnlyFieldA);

            // Then
            assertThat(rows).containsExactly(
                    new Row(Map.of(
                            "key", 1L,
                            "sort", 10L,
                            "A", 100L,
                            "B", 9999999999999999L)));
        }

        @Test
        void shouldNotAffectRequestedFieldsWhenApplyingFilterOnSortKey() throws Exception {
            // Given
            tableProperties.set(FILTERING_CONFIG, "ageOff(sort, 1000)");
            addRootFile("file.parquet", List.of(
                    new Row(Map.of(
                            "key", 1L,
                            "sort", 9999999999999999L,
                            "A", 100L,
                            "B", 1000L,
                            "C", 10000L))));

            // When
            List<Row> rows = getRows(queryOnlyFieldA);

            // Then
            assertThat(rows).containsExactly(
                    new Row(Map.of(
                            "key", 1L,
                            "sort", 9999999999999999L,
                            "A", 100L)));
        }
    }

    @Nested
    @DisplayName("Apply filter iterators")
    class ApplyFilterIterators {

        private final Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(new Field("value", new LongType()))
                .build();

        @BeforeEach
        void setUp() throws Exception {
            tableProperties.setSchema(schema);
            update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            addRootFile("file.parquet", List.of(
                    new Row(Map.of("key", "A", "value", 2L)),
                    new Row(Map.of("key", "B", "value", 9999999999999999L))));
        }

        @Test
        void shouldApplyAgeOffIteratorFromTableProperty() throws Exception {
            // Given
            tableProperties.set(FILTERING_CONFIG, "ageOff(value,1000)");

            // When
            List<Row> rows = getRows(queryAllRows());

            // Then
            assertThat(rows).containsExactly(
                    new Row(Map.of("key", "B", "value", 9999999999999999L)));
        }
    }

    @Nested
    @DisplayName("Apply aggregation iterators")
    class ApplyAggregationIterators {

        private final Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(new Field("value", new IntType()))
                .build();

        @BeforeEach
        void setUp() throws Exception {
            tableProperties.setSchema(schema);
            update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            addRootFile("file.parquet", List.of(
                    new Row(Map.of("key", "A", "value", 2)),
                    new Row(Map.of("key", "A", "value", 4))));
        }

        @Test
        void shouldApplyAggregationFromTableProperty() throws Exception {
            // Given
            tableProperties.set(AGGREGATION_CONFIG, "sum(value)");

            // When
            List<Row> rows = getRows(queryAllRows());

            // Then
            assertThat(rows).containsExactly(
                    new Row(Map.of("key", "A", "value", 6)));
        }
    }

    @Nested
    @DisplayName("Apply iterators by class name")
    class ApplyIteratorsByClassName {

        private final Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(new Field("value", new LongType()))
                .build();

        @BeforeEach
        void setUp() throws Exception {
            tableProperties.setSchema(schema);
            update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            addRootFile("file.parquet", List.of(
                    new Row(Map.of("key", "A", "value", 2L)),
                    new Row(Map.of("key", "A", "value", 2L)),
                    new Row(Map.of("key", "B", "value", 3L)),
                    new Row(Map.of("key", "B", "value", 4L))));
        }

        @Test
        void shouldApplyTableIterator() throws Exception {
            // Given
            tableProperties.set(ITERATOR_CLASS_NAME, AdditionIterator.class.getName());

            // When
            List<Row> rows = getRows(queryAllRows());

            // Then
            assertThat(rows).containsExactly(
                    new Row(Map.of("key", "A", "value", 4L)),
                    new Row(Map.of("key", "B", "value", 7L)));
        }

        @Test
        void shouldApplyQueryIterator() throws Exception {
            // When
            List<Row> rows = getRows(queryAllRowsBuilder()
                    .processingConfig(applyIterator(AdditionIterator.class))
                    .build());

            // Then
            assertThat(rows).containsExactly(
                    new Row(Map.of("key", "A", "value", 4L)),
                    new Row(Map.of("key", "B", "value", 7L)));
        }

        @Test
        void shouldApplyQueryIteratorWithConfig() throws Exception {
            // When
            List<Row> rows = getRows(queryAllRowsBuilder()
                    .processingConfig(applyIterator(SecurityFilteringIterator.class, "key,B"))
                    .build());

            // Then
            assertThat(rows).containsExactly(
                    new Row(Map.of("key", "B", "value", 3L)),
                    new Row(Map.of("key", "B", "value", 4L)));
        }

        @Test
        void shouldApplyTableIteratorThenQueryIterator() throws Exception {
            // Given
            tableProperties.set(ITERATOR_CLASS_NAME, SecurityFilteringIterator.class.getName());
            tableProperties.set(ITERATOR_CONFIG, "key,B");

            // When
            List<Row> rows = getRows(queryAllRowsBuilder()
                    .processingConfig(applyIterator(AdditionIterator.class))
                    .build());

            // Then
            assertThat(rows).containsExactly(
                    new Row(Map.of("key", "B", "value", 7L)));
        }
    }

    @Nested
    @DisplayName("Reinitialise based on a timeout")
    class ReinitialiseOnTimeout {

        @Test
        public void shouldReloadFileReferencesFromStateStoreWhenTimedOut() throws Exception {
            // Given files are added after the executor is first initialised
            tableProperties.set(QUERY_PROCESSOR_CACHE_TIMEOUT, "300");
            QueryExecutorNew queryExecutor = executorAtTime(Instant.parse("2023-11-27T09:30:00Z"));
            addRootFile("file.parquet", List.of(new Row(Map.of("key", 123L))));

            // When the first initialisation has expired
            queryExecutor.initIfNeeded(Instant.parse("2023-11-27T09:35:00Z"));

            // Then the rows that were added are found
            assertThat(getRows(queryExecutor, queryAllRows()))
                    .containsExactly(new Row(Map.of("key", 123L)));
        }

        @Test
        public void shouldNotReloadFileReferencesBeforeTimeOut() throws Exception {
            // Given files are added after the executor is first initialised
            tableProperties.set(QUERY_PROCESSOR_CACHE_TIMEOUT, "300");
            QueryExecutorNew queryExecutor = executorAtTime(Instant.parse("2023-11-27T09:30:00Z"));
            addRootFile("file.parquet", List.of(new Row(Map.of("key", 123L))));

            // When the first initialisation has not yet expired
            queryExecutor.initIfNeeded(Instant.parse("2023-11-27T09:31:00Z"));

            // Then the rows that were added are not found
            assertThat(getRows(queryExecutor, queryAllRows())).isEmpty();
        }
    }
}
