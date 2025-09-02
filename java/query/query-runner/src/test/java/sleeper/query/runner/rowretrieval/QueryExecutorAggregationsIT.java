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
import sleeper.core.schema.type.LongType;
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
        // Given
        Schema schema = getOneValueKeySchema();
        Field field = schema.getRowKeyFields().get(0);
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        StateStore stateStore = initialiseStateStore(tableProperties, new PartitionsBuilder(schema).rootFirst("root").buildList());
        ingestData(instanceProperties, stateStore, tableProperties, getRowsForAggregations().iterator());
        QueryExecutor queryExecutor = queryExecutor(tableProperties, stateStore);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When
        Region region = new Region(rangeFactory.createRange(field, 1L, 1000L));
        Query query = Query.builder()
                .tableName("unused")
                .queryId("abc")
                .regions(List.of(region))
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeAggregations("sum(value1)")
                        .build())
                .build();
        try (CloseableIterator<Row> results = queryExecutor.execute(query)) {

            // Then
            assertThat(results).toIterable().containsExactly(
                    new Row(Map.of("key", 1L, "value1", 10142L)));
        }
    }

    @Test
    public void shouldApplyMinAggregationInQuery() throws IteratorCreationException, ObjectFactoryException, IOException, QueryException {
        // Given
        Schema schema = getOneValueKeySchema();
        Field field = schema.getRowKeyFields().get(0);
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        StateStore stateStore = initialiseStateStore(tableProperties, new PartitionsBuilder(schema).rootFirst("root").buildList());
        ingestData(instanceProperties, stateStore, tableProperties, getRowsForAggregations().iterator());
        QueryExecutor queryExecutor = queryExecutor(tableProperties, stateStore);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When
        Region region = new Region(rangeFactory.createRange(field, 1L, 1000L));
        Query query = Query.builder()
                .tableName("unused")
                .queryId("abc")
                .regions(List.of(region))
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeAggregations("min(value1)")
                        .build())
                .build();
        try (CloseableIterator<Row> results = queryExecutor.execute(query)) {

            // Then
            assertThat(results).toIterable().containsExactly(
                    new Row(Map.of("key", 1L, "value1", 87L)));
        }
    }

    @Test
    public void shouldApplyMaxAggregationInQuery() throws IteratorCreationException, ObjectFactoryException, IOException, QueryException {
        // Given
        Schema schema = getOneValueKeySchema();
        Field field = schema.getRowKeyFields().get(0);
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        StateStore stateStore = initialiseStateStore(tableProperties, new PartitionsBuilder(schema).rootFirst("root").buildList());
        ingestData(instanceProperties, stateStore, tableProperties, getRowsForAggregations().iterator());
        QueryExecutor queryExecutor = queryExecutor(tableProperties, stateStore);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When
        Region region = new Region(rangeFactory.createRange(field, 1L, 1000L));
        Query query = Query.builder()
                .tableName("unused")
                .queryId("abc")
                .regions(List.of(region))
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeAggregations("max(value1)")
                        .build())
                .build();
        try (CloseableIterator<Row> results = queryExecutor.execute(query)) {

            // Then
            assertThat(results).toIterable().containsExactly(
                    new Row(Map.of("key", 1L, "value1", 7841L)));
        }
    }

    protected Schema getOneValueKeySchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new LongType()))
                .valueFields(new Field("value1", new LongType()))
                .build();
    }

    private List<Row> getRowsForAggregations() {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row(Map.of("key", 1L, "value1", 2214L)));
        rows.add(new Row(Map.of("key", 1L, "value1", 87L)));
        rows.add(new Row(Map.of("key", 1L, "value1", 7841L)));
        return rows;
    }
}
