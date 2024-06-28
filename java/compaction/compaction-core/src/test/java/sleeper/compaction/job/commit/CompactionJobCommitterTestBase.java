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
package sleeper.compaction.job.commit;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.compaction.testutils.InMemoryCompactionJobStatusStore;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.InMemoryTableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.ExponentialBackoffWithJitter.Waiter;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static sleeper.compaction.job.status.CompactionJobStartedEvent.compactionJobStarted;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.recordWaits;

public class CompactionJobCommitterTestBase {

    private static final Instant INPUT_UPDATE_TIME = Instant.parse("2024-05-01T10:00:00Z");
    private static final RecordsProcessedSummary DEFAULT_SUMMARY = new RecordsProcessedSummary(
            new RecordsProcessed(120, 100),
            Instant.parse("2024-05-01T10:58:00Z"), Duration.ofMinutes(1));

    private final TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStore();
    protected final Map<String, StateStore> stateStoreByTableId = new HashMap<>();
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final InMemoryCompactionJobStatusStore statusStore = new InMemoryCompactionJobStatusStore();
    protected final TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, tablePropertiesStore);
    protected final List<Duration> foundWaits = new ArrayList<>();
    private Waiter waiter = recordWaits(foundWaits);
    private TableProperties lastTable;

    protected TableProperties createTable() {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
        tablePropertiesStore.createTable(tableProperties);
        stateStoreByTableId.put(tableProperties.get(TABLE_ID), inMemoryStateStoreWithFixedSinglePartition(tableProperties.getSchema()));
        lastTable = tableProperties;
        return tableProperties;
    }

    protected FileReference addInputFile(String filename, long records) throws Exception {
        return addInputFile(lastTable, filename, records);
    }

    protected FileReference addInputFile(TableProperties table, String filename, long records) throws Exception {
        StateStore stateStore = stateStore(table);
        FileReference fileReference = FileReferenceFactory.fromUpdatedAt(stateStore, INPUT_UPDATE_TIME).rootFile(filename, records);
        stateStore.fixFileUpdateTime(INPUT_UPDATE_TIME);
        stateStore.addFile(fileReference);
        return fileReference;
    }

    protected CompactionJob createCompactionJobForOneFileAndRecordStatus(FileReference file) throws Exception {
        return createCompactionJobForOneFileAndRecordStatus(lastTable, file, UUID.randomUUID().toString(), Instant.now());
    }

    protected CompactionJob createCompactionJobForOneFileAndAssign(TableProperties table, FileReference file, String jobId, Instant updateTime) throws Exception {
        CompactionJob job = createCompactionJobForOneFileAndRecordStatus(table, file, jobId, updateTime);
        StateStore stateStore = stateStore(table);
        stateStore.fixFileUpdateTime(updateTime);
        stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles(jobId, file.getPartitionId(), List.of(file.getFilename()))));
        return job;
    }

    protected CompactionJob createCompactionJobForOneFileAndRecordStatus(TableProperties table, FileReference file, String jobId, Instant updateTime) {
        CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, table, List.of(jobId).iterator()::next);
        CompactionJob job = jobFactory.createCompactionJob(List.of(file), file.getPartitionId());
        statusStore.fixUpdateTime(updateTime);
        statusStore.jobCreated(job);
        statusStore.fixUpdateTime(null);
        return job;
    }

    protected CompactionJobCommitRequest runCompactionJobOnTask(String taskId, CompactionJob job) throws Exception {
        return runCompactionJobOnTask(taskId, job, DEFAULT_SUMMARY);
    }

    protected CompactionJobCommitRequest runCompactionJobOnTask(String taskId, CompactionJob job, RecordsProcessedSummary summary) throws Exception {
        statusStore.jobStarted(compactionJobStarted(job, summary.getStartTime()).taskId(taskId).build());
        return new CompactionJobCommitRequest(job, taskId, summary);
    }

    protected CompactionJobCommitter jobCommitter() {
        return new CompactionJobCommitter(statusStore, stateStoreByTableId::get);
    }

    protected FileReferenceFactory fileFactory(TableProperties table, Instant updateTime) {
        StateStore stateStore = stateStore(table);
        return FileReferenceFactory.fromUpdatedAt(stateStore, updateTime);
    }

    protected FileReferenceFactory inputFileFactory() {
        return FileReferenceFactory.fromUpdatedAt(stateStore(), INPUT_UPDATE_TIME);
    }

    protected StateStore stateStore() {
        return stateStore(lastTable);
    }

    protected StateStore stateStore(TableProperties table) {
        return stateStoreByTableId.get(table.get(TABLE_ID));
    }
}
