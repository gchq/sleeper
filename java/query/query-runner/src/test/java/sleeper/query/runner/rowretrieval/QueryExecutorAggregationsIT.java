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
package sleeper.query.runner.rowretrieval;

import org.junit.jupiter.api.Test;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.ObjectFactoryException;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryException;
import sleeper.query.core.model.QueryProcessingConfig;
import sleeper.query.core.rowretrieval.QueryExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;

public class QueryExecutorAggregationsIT extends QueryExecutorITBase {

    @Test
    public void shouldApplySumAggregationInQuery() throws IteratorCreationException, ObjectFactoryException, IOException, QueryException {
        // Given/When
        try (CloseableIterator<Row> results = runBasicOpQuery("sum(value1)")) {

            // Then
            assertThat(results).toIterable().containsExactly(
                    new Row(Map.of("key", 1L, "value1", 10142)),
                    new Row(Map.of("key", 2L, "value1", 123)));
        }
    }

    @Test
    public void shouldApplyMinAggregationInQuery() throws IteratorCreationException, ObjectFactoryException, IOException, QueryException {
        // Given/When
        try (CloseableIterator<Row> results = runBasicOpQuery("min(value1)")) {

            // Then
            assertThat(results).toIterable().containsExactly(
                    new Row(Map.of("key", 1L, "value1", 87)),
                    new Row(Map.of("key", 2L, "value1", 123)));
        }
    }

    @Test
    public void shouldApplyMaxAggregationInQuery() throws IteratorCreationException, ObjectFactoryException, IOException, QueryException {
        // Given/When
        try (CloseableIterator<Row> results = runBasicOpQuery("max(value1)")) {

            // Then
            assertThat(results).toIterable().containsExactly(
                    new Row(Map.of("key", 1L, "value1", 7841)),
                    new Row(Map.of("key", 2L, "value1", 123)));
        }
    }

    @Test
    public void shouldApplyMultipleAggregationsInQuery() throws IteratorCreationException, ObjectFactoryException, IOException, QueryException {
        // Given/When
        try (CloseableIterator<Row> results = runMultiBasicOpQuery("sum(value1),max(value2),min(value3)")) {

            // Then
            assertThat(results).toIterable().containsExactly(
                    new Row(Map.of("key", 1L, "value1", 1110, "value2", 1000, "value3", 10)),
                    new Row(Map.of("key", 2L, "value1", 10, "value2", 10, "value3", 10)));
        }
    }

    @Test
    void shouldApplyMapSumAggregationInQuery() throws IteratorCreationException, IOException, QueryException {
        //Given/When
        try (CloseableIterator<Row> results = runMapQuery("map_Sum(map_value)")) {

            // Then
            assertThat(results).toIterable().containsExactly(
                    new Row(new Row(Map.of("key", 1L, "map_value",
                            Map.of("map_key1", 4, "map_key2", 7)))),
                    new Row(new Row(Map.of("key", 2L, "map_value",
                            Map.of("map_key1", 3, "map_key2", 4)))));
        }
    }

    @Test
    void shouldApplyMapMaxAggregationInQuery() throws IteratorCreationException, IOException, QueryException {
        //Given/When
        try (CloseableIterator<Row> results = runMapQuery("map_Max(map_value)")) {

            // Then
            assertThat(results).toIterable().containsExactly(
                    new Row(new Row(Map.of("key", 1L, "map_value",
                            Map.of("map_key1", 3, "map_key2", 4)))),
                    new Row(new Row(Map.of("key", 2L, "map_value",
                            Map.of("map_key1", 3, "map_key2", 4)))));
        }
    }

    @Test
    void shouldApplyMapMinAggregationInQuery() throws IteratorCreationException, IOException, QueryException {
        //Given/When
        try (CloseableIterator<Row> results = runMapQuery("map_Min(map_value)")) {

            // Then
            assertThat(results).toIterable().containsExactly(
                    new Row(new Row(Map.of("key", 1L, "map_value",
                            Map.of("map_key1", 1, "map_key2", 3)))),
                    new Row(new Row(Map.of("key", 2L, "map_value",
                            Map.of("map_key1", 3, "map_key2", 4)))));
        }
    }

    private CloseableIterator<Row> runBasicOpQuery(String aggregations) throws IOException, IteratorCreationException, QueryException {
        return runQuery(getOneValueKeySchema(), getRowsForBasicAggregations(), aggregations);
    }

    private CloseableIterator<Row> runMultiBasicOpQuery(String aggregations) throws IOException, IteratorCreationException, QueryException {
        return runQuery(getMultiValueKeySchema(), getRowsForMultiAggregations(), aggregations);
    }

    private CloseableIterator<Row> runMapQuery(String aggregations) throws IOException, IteratorCreationException, QueryException {
        return runQuery(getMapValueKeySchema(), getRowsForMapAggregations(), aggregations);
    }

    private CloseableIterator<Row> runQuery(Schema schema, List<Row> tableRows, String aggregations) throws IOException, IteratorCreationException, QueryException {
        Field field = schema.getRowKeyFields().get(0);
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        StateStore stateStore = initialiseStateStore(tableProperties, new PartitionsBuilder(schema).rootFirst("root").buildList());
        ingestData(instanceProperties, stateStore, tableProperties, tableRows.iterator());
        QueryExecutor queryExecutor = queryExecutor(tableProperties, stateStore);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When
        Region region = new Region(rangeFactory.createRange(field, 1L, 1000L));
        Query query = buildQuery(region, aggregations);

        return queryExecutor.execute(query);
    }

    private Query buildQuery(Region region, String aggregations) {
        return Query.builder()
                .tableName("unused")
                .queryId("abc")
                .regions(List.of(region))
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeAggregations(aggregations)
                        .build())
                .build();
    }

    private Schema getOneValueKeySchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new LongType()))
                .valueFields(new Field("value1", new IntType()))
                .build();
    }

    private List<Row> getRowsForBasicAggregations() {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row(Map.of("key", 1L, "value1", 2214)));
        rows.add(new Row(Map.of("key", 1L, "value1", 87)));
        rows.add(new Row(Map.of("key", 1L, "value1", 7841)));
        rows.add(new Row(Map.of("key", 2L, "value1", 123))); //Different row key shouldn't be aggregated
        return rows;
    }

    private Schema getMultiValueKeySchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new LongType()))
                .valueFields(new Field("value1", new IntType()), new Field("value2", new IntType()), new Field("value3", new IntType()))
                .build();
    }

    private List<Row> getRowsForMultiAggregations() {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row(Map.of("key", 1L, "value1", 10, "value2", 10, "value3", 10)));
        rows.add(new Row(Map.of("key", 1L, "value1", 100, "value2", 100, "value3", 100)));
        rows.add(new Row(Map.of("key", 1L, "value1", 1000, "value2", 1000, "value3", 1000)));
        rows.add(new Row(Map.of("key", 2L, "value1", 10, "value2", 10, "value3", 10))); //Different row key shouldn't be aggregated
        return rows;
    }

    private Schema getMapValueKeySchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new LongType()))
                .valueFields(new Field("map_value", new MapType(new StringType(), new IntType())))
                .build();
    }

    private List<Row> getRowsForMapAggregations() {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row(Map.of("key", 1L, "map_value",
                Map.of("map_key1", 1, "map_key2", 3))));
        rows.add(new Row(Map.of("key", 1L, "map_value",
                Map.of("map_key1", 3, "map_key2", 4))));
        rows.add(new Row(Map.of("key", 2L, "map_value", //Different row key shouldn't be aggregated
                Map.of("map_key1", 3, "map_key2", 4))));
        return rows;
    }
}
