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

import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.row.testutils.InMemoryRowStore;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.util.ObjectFactory;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryException;
import sleeper.query.core.model.QueryProcessingConfig;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Spliterator.IMMUTABLE;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class QueryExecutorTestBase {
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final InMemoryRowStore rowStore = new InMemoryRowStore();
    protected final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key", new LongType()));
    protected final StateStore stateStore = InMemoryTransactionLogStateStore.createAndInitialise(tableProperties, new InMemoryTransactionLogs());

    protected void addRootFile(String filename, List<Row> rows) {
        addFile(fileReferenceFactory().rootFile(filename, rows.size()), rows);
    }

    protected void addPartitionFile(String partitionId, String filename, List<Row> rows) {
        addFile(fileReferenceFactory().partitionFile(partitionId, filename, rows.size()), rows);
    }

    protected void addFile(FileReference fileReference, List<Row> rows) {
        addFileMetadata(fileReference);
        rowStore.addFile(fileReference.getFilename(), rows);
    }

    protected void addFileMetadata(FileReference fileReference) {
        update(stateStore).addFile(fileReference);
    }

    protected QueryPlanner planner() throws Exception {
        return plannerAtTime(Instant.now());
    }

    protected QueryExecutor executorAtTime(Instant time) throws Exception {
        return new QueryExecutor(plannerAtTime(time), leafQueryExecutor());
    }

    protected List<Row> getRows(Query query) throws Exception {
        return getRows(new QueryExecutor(planner(), leafQueryExecutor()), query);
    }

    protected List<Row> getRows(QueryExecutor executor, Query query) {
        try (var it = executor.execute(query)) {
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, IMMUTABLE), false)
                    .collect(Collectors.toUnmodifiableList());
        } catch (QueryException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private QueryPlanner plannerAtTime(Instant time) throws Exception {
        QueryPlanner planner = new QueryPlanner(tableProperties, stateStore, time);
        planner.init(time);
        return planner;
    }

    private LeafPartitionQueryExecutor leafQueryExecutor() {
        return new LeafPartitionQueryExecutor(ObjectFactory.noUserJars(), tableProperties, new InMemoryLeafPartitionRowRetriever(rowStore));
    }

    protected Query.Builder query() {
        return Query.builder().queryId(UUID.randomUUID().toString())
                .tableName(tableProperties.get(TABLE_NAME));
    }

    protected Query queryAllRows() {
        return queryAllRowsBuilder().build();
    }

    protected Query.Builder queryAllRowsBuilder() {
        return query()
                .regions(List.of(partitionTree().getRootPartition().getRegion()));
    }

    protected Query queryRange(Object min, Object max) {
        return queryRegions(range(min, max));
    }

    protected Query queryRegions(Region... regions) {
        return query()
                .regions(List.of(regions))
                .build();
    }

    protected Region range(Object min, Object max) {
        return new Region(rangeFactory().createRange("key", min, max));
    }

    protected RangeFactory rangeFactory() {
        return new RangeFactory(tableProperties.getSchema());
    }

    protected PartitionTree partitionTree() {
        return new PartitionTree(stateStore.getAllPartitions());
    }

    protected FileReferenceFactory fileReferenceFactory() {
        return FileReferenceFactory.from(stateStore);
    }

    protected Region rootPartitionRegion() {
        return new PartitionTree(stateStore.getAllPartitions()).getRootPartition().getRegion();
    }

    protected static QueryProcessingConfig requestValueFields(String... fields) {
        return QueryProcessingConfig.builder()
                .requestedValueFields(List.of(fields))
                .build();
    }

    protected static QueryProcessingConfig applyIterator(Class<?> iteratorClass) {
        return QueryProcessingConfig.builder()
                .queryTimeIteratorClassName(iteratorClass.getName())
                .build();
    }

    protected static QueryProcessingConfig applyIterator(Class<?> iteratorClass, String config) {
        return QueryProcessingConfig.builder()
                .queryTimeIteratorClassName(iteratorClass.getName())
                .queryTimeIteratorConfig(config)
                .build();
    }
}
