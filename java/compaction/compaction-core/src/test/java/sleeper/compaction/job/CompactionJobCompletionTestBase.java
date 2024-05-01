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
package sleeper.compaction.job;

import sleeper.compaction.testutils.InMemoryCompactionJobStatusStore;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.InMemoryTableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.statestore.FixedStateStoreProvider;
import sleeper.statestore.StateStoreProvider;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.noJitter;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.noWaits;

public class CompactionJobCompletionTestBase {

    private final TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStore();
    private final Map<String, StateStore> stateStoreByTableName = new HashMap<>();
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final InMemoryCompactionJobStatusStore statusStore = new InMemoryCompactionJobStatusStore();
    protected final TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, tablePropertiesStore);
    protected final StateStoreProvider stateStoreProvider = new FixedStateStoreProvider(stateStoreByTableName);

    protected TableProperties createTable() {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
        tablePropertiesStore.createTable(tableProperties);
        stateStoreByTableName.put(tableProperties.get(TABLE_NAME), inMemoryStateStoreWithFixedSinglePartition(tableProperties.getSchema()));
        return tableProperties;
    }

    protected FileReference addInputFile(TableProperties table, String filename, long records) throws Exception {
        StateStore stateStore = stateStoreProvider.getStateStore(table);
        FileReference fileReference = FileReferenceFactory.from(stateStore).rootFile(filename, records);
        stateStore.addFile(fileReference);
        return fileReference;
    }

    protected CompactionJob createCompactionJobForOneFile(TableProperties table, FileReference file, String jobId, Instant updateTime) throws Exception {
        CompactionJob job = createCompactionJobForOneFileNoJobAssignment(table, file, jobId, updateTime);
        StateStore stateStore = stateStoreProvider.getStateStore(table);
        stateStore.fixFileUpdateTime(updateTime);
        stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles(jobId, file.getPartitionId(), List.of(file.getFilename()))));
        return job;
    }

    protected CompactionJob createCompactionJobForOneFileNoJobAssignment(TableProperties table, FileReference file, String jobId, Instant updateTime) {
        CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, table, () -> jobId);
        CompactionJob job = jobFactory.createCompactionJob(List.of(file), file.getPartitionId());
        statusStore.fixUpdateTime(updateTime);
        statusStore.jobCreated(job);
        statusStore.fixUpdateTime(null);
        return job;
    }

    protected CompactionJobRunCompleted runCompactionJobOnTask(String taskId, CompactionJob job, RecordsProcessedSummary summary) throws Exception {
        statusStore.jobStarted(job, summary.getStartTime(), taskId);
        return new CompactionJobRunCompleted(job, taskId, summary);
    }

    protected CompactionJobCompletion completionWithUpdateTime(TableProperties table, Instant updateTime) {
        StateStore stateStore = stateStoreProvider.getStateStore(table);
        return completionWithUpdateTime(statusStore, stateStore, updateTime);
    }

    protected CompactionJobCompletion completionWithUpdateTime(CompactionJobStatusStore statusStore, StateStore stateStore, Instant updateTime) {
        stateStore.fixFileUpdateTime(updateTime);
        return new CompactionJobCompletion(statusStore, stateStore,
                CompactionJobCompletion.JOB_ASSIGNMENT_WAIT_ATTEMPTS, new ExponentialBackoffWithJitter(
                        CompactionJobCompletion.JOB_ASSIGNMENT_WAIT_RANGE,
                        noJitter(), noWaits()));
    }

    protected FileReferenceFactory fileFactory(TableProperties table, Instant updateTime) {
        StateStore stateStore = stateStoreProvider.getStateStore(table);
        return FileReferenceFactory.fromUpdatedAt(stateStore, updateTime);
    }
}
