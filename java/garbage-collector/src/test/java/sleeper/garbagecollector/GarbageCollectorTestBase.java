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
package sleeper.garbagecollector;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.garbagecollector.FailedGarbageCollectionException.FileFailure;
import sleeper.garbagecollector.FailedGarbageCollectionException.TableFailures;
import sleeper.garbagecollector.GarbageCollector.DeleteFiles;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class GarbageCollectorTestBase {

    private static final Schema TEST_SCHEMA = getSchema();

    protected final PartitionTree partitions = new PartitionsBuilder(TEST_SCHEMA).singlePartition("root").buildTree();
    protected final List<TableProperties> tables = new ArrayList<>();
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore
            .createProvider(instanceProperties, new InMemoryTransactionLogsPerTable());

    protected final List<StateStoreCommitRequest> sentCommits = new ArrayList<>();
    protected final Set<String> filesInBucket = new HashSet<>();

    protected static TableFailures fileFailure(TableProperties table, String filename, Exception failure) {
        return new TableFailures(table.getStatus(), null,
                List.of(new FileFailure(List.of(filename), failure)),
                List.of());
    }

    protected FileReferenceFactory fileReferenceFactory() {
        return FileReferenceFactory.from(partitions);
    }

    protected FileReference createActiveFile(String filename, StateStore stateStore) throws Exception {
        FileReference fileReference = fileReferenceFactory().rootFile(filename, 100L);
        update(stateStore).addFile(fileReference);
        filesInBucket.add(filename);
        return fileReference;
    }

    protected void createFileWithNoReferencesByCompaction(StateStore stateStore,
            String oldFilePath, String newFilePath) throws Exception {
        FileReference oldFile = createActiveFile(oldFilePath, stateStore);
        filesInBucket.add(newFilePath);
        update(stateStore).assignJobIds(List.of(
                assignJobOnPartitionToFiles("job1", "root", List.of(oldFile.getFilename()))));
        update(stateStore).atomicallyReplaceFileReferencesWithNewOnes(List.of(replaceJobFileReferences(
                "job1", List.of(oldFile.getFilename()), fileReferenceFactory().rootFile(newFilePath.toString(), 100))));
    }

    protected FileReference activeReference(String filePath) {
        return fileReferenceFactory().rootFile(filePath, 100);
    }

    protected TableProperties createTable() {
        return createTable(createTestTableProperties(instanceProperties, TEST_SCHEMA));
    }

    protected TableProperties createTableWithId(String tableId) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, TEST_SCHEMA);
        tableProperties.set(TABLE_ID, tableId);
        return createTable(tableProperties);
    }

    protected TableProperties createTable(TableProperties tableProperties) {
        tables.add(tableProperties);
        update(stateStoreProvider.getStateStore(tableProperties)).initialise(partitions.getAllPartitions());
        return tableProperties;
    }

    protected TableProperties createTableWithGcDelayMinutes(int delay) {
        TableProperties tableProperties = createTable();
        tableProperties.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, delay);
        return tableProperties;
    }

    protected StateStore stateStore(TableProperties table) {
        return stateStoreProvider.getStateStore(table);
    }

    protected StateStore stateStoreWithFixedTime(TableProperties table, Instant fixedTime) {
        StateStore store = stateStore(table);
        store.fixFileUpdateTime(fixedTime);
        return store;
    }

    protected int collectGarbageAtTime(Instant time) throws Exception {
        return collectorNew().runAtTime(time, tables);
    }

    protected GarbageCollector collectorNew() throws Exception {
        return new GarbageCollector(deleteAllFilesSuccessfully(), instanceProperties, stateStoreProvider, sentCommits::add);
    }

    protected GarbageCollector collectorWithDeleteAction(DeleteFiles deleteFiles) throws Exception {
        return new GarbageCollector(deleteFiles, instanceProperties, stateStoreProvider, sentCommits::add);
    }

    protected DeleteFiles deleteAllFilesSuccessfully() {
        return (filenames, deleted) -> {
            filesInBucket.removeAll(filenames);
            filenames.forEach(deleted::deleted);
        };
    }

    protected DeleteFiles deleteAllFilesExcept(String failFilename, Exception failure) {
        return (filenames, deleted) -> {
            for (String filename : filenames) {
                if (failFilename.equals(filename)) {
                    deleted.failed(List.of(filename), failure);
                } else {
                    deleted.deleted(filename);
                    filesInBucket.remove(filename);
                }
            }
        };
    }

    private static Schema getSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new StringType()))
                .build();
    }
}
