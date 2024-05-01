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
package sleeper.compaction.completion.lambda;

import org.junit.jupiter.api.Test;

import sleeper.compaction.completion.lambda.CompactionJobCompletionLambda.CompactionJobCompletionConstructor;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobCompletion;
import sleeper.compaction.job.CompactionJobCompletionRequest;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.compaction.job.CompactionJobRunCompleted;
import sleeper.compaction.job.status.CompactionJobStatus;
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
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.statestore.FixedStateStoreProvider;
import sleeper.statestore.StateStoreProvider;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.noJitter;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.noWaits;

public class CompactionJobCompletionLambdaTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Map<String, StateStore> stateStoreByTableName = new HashMap<>();
    private final InMemoryCompactionJobStatusStore statusStore = new InMemoryCompactionJobStatusStore();
    private final TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStore();
    private final TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, tablePropertiesStore);
    private final StateStoreProvider stateStoreProvider = new FixedStateStoreProvider(stateStoreByTableName);

    @Test
    void shouldCompleteCompactionJobsOnDifferentTables() throws Exception {
        // Given
        TableProperties table1 = createTable();
        TableProperties table2 = createTable();
        FileReference file1 = addInputFile(table1, "file1.parquet", 123, Instant.parse("2024-05-01T10:42:00Z"));
        FileReference file2 = addInputFile(table2, "file2.parquet", 456, Instant.parse("2024-05-01T10:42:30Z"));
        CompactionJob job1 = createCompactionJobForOneFile(table1, file1, "job-1", Instant.parse("2024-05-01T10:50:00Z"));
        CompactionJob job2 = createCompactionJobForOneFile(table2, file2, "job-2", Instant.parse("2024-05-01T10:50:30Z"));
        RecordsProcessedSummary summary1 = new RecordsProcessedSummary(
                new RecordsProcessed(120, 100),
                Instant.parse("2024-05-01T10:58:00Z"), Duration.ofMinutes(1));
        RecordsProcessedSummary summary2 = new RecordsProcessedSummary(
                new RecordsProcessed(450, 400),
                Instant.parse("2024-05-01T10:58:30Z"), Duration.ofMinutes(1));
        CompactionJobRunCompleted completion1 = runCompactionJobOnTask("task-1", job1, summary1);
        CompactionJobRunCompleted completion2 = runCompactionJobOnTask("task-2", job2, summary2);

        // When
        lambdaWithUpdateTimes(List.of(Instant.parse("2024-05-01T11:00:00Z"), Instant.parse("2024-05-01T11:00:30Z")))
                .completeJobs(new CompactionJobCompletionRequest(List.of(completion1, completion2)));

        // Then
        StateStore state1 = stateStoreProvider.getStateStore(table1);
        StateStore state2 = stateStoreProvider.getStateStore(table2);
        CompactionJobStatus status1 = statusStore.getJob(job1.getId()).orElseThrow();
        CompactionJobStatus status2 = statusStore.getJob(job2.getId()).orElseThrow();
        assertThat(status1).isEqualTo(jobCreated(job1,
                Instant.parse("2024-05-01T10:50:00Z"),
                finishedCompactionRun("task-1", summary1)));
        assertThat(status2).isEqualTo(jobCreated(job2,
                Instant.parse("2024-05-01T10:50:30Z"),
                finishedCompactionRun("task-2", summary2)));
        assertThat(state1.getFileReferences()).containsExactly(
                fileFactory(table1, Instant.parse("2024-05-01T11:00:00Z"))
                        .rootFile(job1.getOutputFile(), 100));
        assertThat(state2.getFileReferences()).containsExactly(
                fileFactory(table2, Instant.parse("2024-05-01T11:00:30Z"))
                        .rootFile(job2.getOutputFile(), 400));
    }

    private TableProperties createTable() {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
        tablePropertiesStore.createTable(tableProperties);
        stateStoreByTableName.put(tableProperties.get(TABLE_NAME), inMemoryStateStoreWithFixedSinglePartition(tableProperties.getSchema()));
        return tableProperties;
    }

    private FileReference addInputFile(TableProperties table, String filename, long records, Instant updateTime) throws Exception {
        StateStore stateStore = stateStoreProvider.getStateStore(table);
        FileReference fileReference = FileReferenceFactory.fromUpdatedAt(stateStore, updateTime).rootFile(filename, records);
        stateStore.fixFileUpdateTime(updateTime);
        stateStore.addFile(fileReference);
        return fileReference;
    }

    private CompactionJob createCompactionJobForOneFile(TableProperties table, FileReference file, String jobId, Instant updateTime) throws Exception {
        CompactionJob job = createCompactionJobForOneFileNoJobAssignment(table, file, jobId, updateTime);
        StateStore stateStore = stateStoreProvider.getStateStore(table);
        stateStore.fixFileUpdateTime(updateTime);
        stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles(jobId, file.getPartitionId(), List.of(file.getFilename()))));
        return job;
    }

    private CompactionJob createCompactionJobForOneFileNoJobAssignment(TableProperties table, FileReference file, String jobId, Instant updateTime) {
        CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, table, () -> jobId);
        CompactionJob job = jobFactory.createCompactionJob(List.of(file), file.getPartitionId());
        statusStore.fixUpdateTime(updateTime);
        statusStore.jobCreated(job);
        statusStore.fixUpdateTime(null);
        return job;
    }

    private CompactionJobRunCompleted runCompactionJobOnTask(String taskId, CompactionJob job, RecordsProcessedSummary summary) throws Exception {
        statusStore.jobStarted(job, summary.getStartTime(), taskId);
        return new CompactionJobRunCompleted(job, taskId, summary);
    }

    private CompactionJobCompletionLambda lambdaWithUpdateTimes(List<Instant> updateTimes) {
        return new CompactionJobCompletionLambda(tablePropertiesProvider, stateStoreProvider, statusStore, completionWithUpdateTimes(updateTimes));
    }

    private CompactionJobCompletionConstructor completionWithUpdateTimes(List<Instant> updateTimes) {
        Iterator<Instant> timeIterator = updateTimes.iterator();
        return (statusStore, stateStore) -> {
            Instant updateTime = timeIterator.next();
            stateStore.fixFileUpdateTime(updateTime);
            return new CompactionJobCompletion(statusStore, stateStore,
                    CompactionJobCompletion.JOB_ASSIGNMENT_WAIT_ATTEMPTS, new ExponentialBackoffWithJitter(
                            CompactionJobCompletion.JOB_ASSIGNMENT_WAIT_RANGE,
                            noJitter(), noWaits()),
                    () -> updateTime);
        };
    }

    private FileReferenceFactory fileFactory(TableProperties table, Instant updateTime) {
        StateStore stateStore = stateStoreProvider.getStateStore(table);
        return FileReferenceFactory.fromUpdatedAt(stateStore, updateTime);
    }

}
