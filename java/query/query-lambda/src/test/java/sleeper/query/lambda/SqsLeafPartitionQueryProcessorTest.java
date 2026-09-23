/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.query.lambda;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.row.testutils.InMemoryRowStore;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.runner.testutils.InMemoryIngest;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.rowretrieval.InMemoryLeafPartitionRowRetriever;
import sleeper.query.core.rowretrieval.InMemoryResultsOutput;
import sleeper.query.core.rowretrieval.QueryPlanner;
import sleeper.query.core.tracker.InMemoryQueryTracker;
import sleeper.query.core.tracker.QueryState;
import sleeper.query.core.tracker.TrackedQuery;
import sleeper.sketches.testutils.InMemorySketchesStore;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.testutils.SupplierTestHelper.timePassesAMinuteAtATimeFrom;

public class SqsLeafPartitionQueryProcessorTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key", new StringType()));
    private final InMemoryRowStore rowStore = new InMemoryRowStore();
    private final StateStore stateStore = InMemoryTransactionLogStateStore.createAndInitialise(tableProperties, new InMemoryTransactionLogs());
    private final InMemoryIngest ingest = new InMemoryIngest(instanceProperties, tableProperties, stateStore, rowStore, new InMemorySketchesStore());
    private final InMemoryLeafPartitionRowRetriever rowRetriever = new InMemoryLeafPartitionRowRetriever(rowStore);
    private final InMemoryResultsOutput resultsOutput = new InMemoryResultsOutput();
    private final InMemoryQueryTracker queryTracker = new InMemoryQueryTracker(instanceProperties, timePassesAMinuteAtATimeFrom(Instant.parse("2026-09-23T11:30:00Z")));

    @Test
    void shouldRetrieveSingleRow() throws Exception {
        // Given
        Row row = new Row(Map.of("key", "test"));
        ingest.write(List.of(row));

        // When
        executeQueryByRange(rangeFactory().createRangeCoveringAllValues("key"));

        // Then the row is sent to the results output
        assertThat(resultsOutput.streamPublishedResults()).containsExactly(row);
        // And the row iterator is closed
        assertThat(rowRetriever.getIteratorsOpened()).isEqualTo(1);
        assertThat(rowRetriever.getIteratorsClosed()).isEqualTo(1);
        // And the leaf query completion is tracked
        assertThat(queryTracker.getAllQueries())
                .extracting(TrackedQuery::getLastKnownState)
                .containsExactly(QueryState.COMPLETED);
    }

    private RangeFactory rangeFactory() {
        return new RangeFactory(tableProperties.getSchema());
    }

    private void executeQueryByRange(Range range) throws Exception {
        executeQuery(Query.builder()
                .tableName(tableProperties.get(TABLE_NAME))
                .regions(List.of(new Region(range)))
                .build());
    }

    private void executeQuery(Query query) {
        List<LeafPartitionQuery> subQueries = QueryPlanner.initialiseNow(tableProperties, stateStore).splitIntoLeafPartitionQueries(query);
        SqsLeafPartitionQueryProcessor processor = createProcessor();
        for (LeafPartitionQuery subQuery : subQueries) {
            processor.processQuery(subQuery);
        }
    }

    private SqsLeafPartitionQueryProcessor createProcessor() {
        return SqsLeafPartitionQueryProcessor.builder()
                .tablePropertiesProvider(new FixedTablePropertiesProvider(tableProperties))
                .rowRetrieverProvider(rowRetriever)
                .resultsOutputProvider(resultsOutput)
                .objectFactory(ObjectFactory.noUserJars())
                .queryTracker(queryTracker)
                .build();
    }

}
