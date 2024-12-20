/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.bulkexport.core.recordretrieval;

import org.junit.jupiter.api.Test;

import sleeper.bulkexport.core.model.BulkExportLeafPartitionQuery;
import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.QUERY_PROCESSOR_CACHE_TIMEOUT;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreTestHelper.inMemoryStateStoreWithSinglePartition;

public class BulkExportQuerySplitterTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key");
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final StateStore stateStore = inMemoryStateStoreWithSinglePartition(schema);

    private final List<Record> right = List.of(
            new Record(Map.of("key", 2L)),
            new Record(Map.of("key", 7L)));
    private final List<Record> left = List.of(
            new Record(Map.of("key", 8L)),
            new Record(Map.of("key", 13L)));
    private final List<Record> leftleft = List.of(
            new Record(Map.of("key", 8L)),
            new Record(Map.of("key", 13L)));
    private final List<Record> leftright = List.of(
            new Record(Map.of("key", 15)),
            new Record(Map.of("key", 20L)));

    @Test
    public void shouldExportWholeTree() throws Exception {
        // Given
        BulkExportQuery bulkExportQuery = bulkExportQuery();
        stateStore.initialise(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 5L)
                .splitToNewChildren("L", "LL", "LR", 15L)
                .buildList());
        addRootFile("root.parquet", List.of(new Record(Map.of("key", 123L))));
        addPartitionFile("R", "right.parquet", right);
        addPartitionFile("L", "left.parquet", left);
        addPartitionFile("LL", "leftleft.parquet", leftleft);
        addPartitionFile("LR", "leftright.parquet", leftright);
        BulkExportQuerySplitter bulkExportQuerySplitter = executor();

        // When
        List<BulkExportLeafPartitionQuery> leafPartitionQueries = bulkExportQuerySplitter
                .splitIntoLeafPartitionQueries(bulkExportQuery);

        // That
        assertThat(leafPartitionQueries).hasSize(3);

    }

    @Test
    public void shouldProduceTwoBulkExportLeafPartitionQueries() throws Exception {
        // Given
        BulkExportQuery bulkExportQuery = bulkExportQuery();
        stateStore.initialise(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 5L)
                .buildList());
        addPartitionFile("R", "right.parquet", right);
        addPartitionFile("L", "left.parquet", left);
        BulkExportQuerySplitter bulkExportQuerySplitter = executor();

        // When
        List<BulkExportLeafPartitionQuery> leafPartitionQueries = bulkExportQuerySplitter
                .splitIntoLeafPartitionQueries(bulkExportQuery);

        // That
        assertThat(leafPartitionQueries).hasSize(2);
    }

    @Test
    public void shouldReloadActiveFilesFromStateStoreWhenTimedOut() throws Exception {
        // Given files are added after the executor is first initialised
        BulkExportQuery bulkExportQuery = bulkExportQuery();
        tableProperties.set(QUERY_PROCESSOR_CACHE_TIMEOUT, "5");
        BulkExportQuerySplitter bulkExportQuerySplitter = executorAtTime(Instant.parse("2023-11-27T09:30:00Z"));
        addRootFile("file.parquet", List.of(new Record(Map.of("key", 123L))));

        // When the first initialisation has expired
        bulkExportQuerySplitter.initIfNeeded(Instant.parse("2023-11-27T09:35:00Z"));

        // Then active files are reloaded
        List<BulkExportLeafPartitionQuery> leafPartitionQueries = bulkExportQuerySplitter
                .splitIntoLeafPartitionQueries(bulkExportQuery);

        assertThat(leafPartitionQueries).hasSize(1);
    }

    @Test
    public void shouldNotReloadActiveFilesBeforeTimeOut() throws Exception {
        // Given files are added after the executor is first initialised
        BulkExportQuery bulkExportQuery = bulkExportQuery();
        tableProperties.set(QUERY_PROCESSOR_CACHE_TIMEOUT, "5");
        BulkExportQuerySplitter bulkExportQuerySplitter = executorAtTime(Instant.parse("2023-11-27T09:30:00Z"));
        addRootFile("file.parquet", List.of(new Record(Map.of("key", 123L))));

        // When the first initialisation has not yet expired
        bulkExportQuerySplitter.initIfNeeded(Instant.parse("2023-11-27T09:31:00Z"));

        // Then the records that were added are not found
        List<BulkExportLeafPartitionQuery> leafPartitionQueries = bulkExportQuerySplitter
                .splitIntoLeafPartitionQueries(bulkExportQuery);

        assertThat(leafPartitionQueries).isEmpty();
    }

    private BulkExportQuerySplitter executor() throws Exception {
        return executorAtTime(Instant.now());
    }

    private BulkExportQuerySplitter executorAtTime(Instant time) throws Exception {
        BulkExportQuerySplitter executor = uninitialisedExecutorAtTime(time);
        executor.init(time);
        return executor;
    }

    private BulkExportQuerySplitter uninitialisedExecutorAtTime(Instant time) {
        return new BulkExportQuerySplitter(stateStore, tableProperties, time);
    }

    private void addRootFile(String filename, List<Record> records) {
        addFile(fileReferenceFactory().rootFile(filename, records.size()), records);
    }

    private void addPartitionFile(String partitionId, String filename, List<Record> records) {
        addFile(fileReferenceFactory().partitionFile(partitionId, filename, records.size()), records);
    }

    private void addFile(FileReference fileReference, List<Record> records) {
        addFileMetadata(fileReference);
    }

    private void addFileMetadata(FileReference fileReference) {
        stateStore.addFile(fileReference);
    }

    private BulkExportQuery bulkExportQuery() {
        return BulkExportQuery.builder().exportId(UUID.randomUUID().toString())
                .tableName(tableProperties.get(TABLE_NAME)).build();
    }

    private FileReferenceFactory fileReferenceFactory() {
        return FileReferenceFactory.from(stateStore);
    }
}
