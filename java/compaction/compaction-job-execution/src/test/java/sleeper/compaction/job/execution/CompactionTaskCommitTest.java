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
package sleeper.compaction.job.execution;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.task.CompactionTaskFinishedStatus;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.FixedStateStoreProvider;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionFailedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionFinishedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.failedCompactionRun;
import static sleeper.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.CompactionJobStatusTestData.startedCompactionRun;
import static sleeper.compaction.job.status.CompactionJobFailedEvent.compactionJobFailed;
import static sleeper.compaction.job.status.CompactionJobFinishedEvent.compactionJobFinished;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_JOB_COMMIT_ASYNC;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithSinglePartition;

public class CompactionTaskCommitTest extends CompactionTaskTestBase {
    private final TableProperties table1 = createTable("test-table-1-id", "test-table-1");
    private final TableProperties table2 = createTable("test-table-2-id", "test-table-2");
    private final StateStore store1 = inMemoryStateStoreWithSinglePartition(schema);
    private final StateStore store2 = inMemoryStateStoreWithSinglePartition(schema);
    private final FixedTablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(List.of(table1, table2));
    private final FixedStateStoreProvider stateStoreProvider = new FixedStateStoreProvider(Map.of("test-table-1", store1, "test-table-2", store2));

    @Nested
    @DisplayName("Send commits to queue")
    class SendCommitsToQueue {

        @Test
        void shouldSendJobCommitRequestToQueue() throws Exception {
            // Given
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"),   // Start
                    Instant.parse("2024-02-22T13:50:01Z"),   // Job started
                    Instant.parse("2024-02-22T13:50:02Z"),   // Job completed
                    Instant.parse("2024-02-22T13:50:05Z"))); // Finish
            CompactionJob job1 = createJobOnQueue("job1");
            RecordsProcessed job1Summary = new RecordsProcessed(10L, 5L);

            // When
            runTask(processJobs(jobSucceeds(job1Summary)), times::poll);

            // Then
            assertThat(successfulJobs).containsExactly(job1);
            assertThat(failedJobs).isEmpty();
            assertThat(jobsOnQueue).isEmpty();
            assertThat(commitRequestsOnQueue).containsExactly(
                    commitRequestFor(job1,
                            new RecordsProcessedSummary(job1Summary,
                                    Instant.parse("2024-02-22T13:50:01Z"),
                                    Instant.parse("2024-02-22T13:50:02Z"))));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                    jobCreated(job1, DEFAULT_CREATED_TIME,
                            startedCompactionRun(DEFAULT_TASK_ID,
                                    Instant.parse("2024-02-22T13:50:01Z"))));
        }

        @Test
        void shouldSendJobCommitRequestsForDifferentTablesToQueue() throws Exception {
            // Given
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"),   // Start
                    Instant.parse("2024-02-22T13:50:01Z"),   // Job 1 started
                    Instant.parse("2024-02-22T13:50:02Z"),   // Job 1 completed
                    Instant.parse("2024-02-22T13:50:03Z"),   // Job 2 started
                    Instant.parse("2024-02-22T13:50:04Z"),   // Job 2 completed
                    Instant.parse("2024-02-22T13:50:07Z"))); // Finish
            CompactionJob job1 = createJobOnQueue("job1", table1, store1);
            RecordsProcessed job1Summary = new RecordsProcessed(10L, 10L);
            CompactionJob job2 = createJobOnQueue("job2", table2, store2);
            RecordsProcessed job2Summary = new RecordsProcessed(20L, 20L);

            // When
            runTask(processJobs(
                    jobSucceeds(job1Summary),
                    jobSucceeds(job2Summary)),
                    times::poll, tablePropertiesProvider, stateStoreProvider);

            // Then
            assertThat(successfulJobs).containsExactly(job1, job2);
            assertThat(failedJobs).isEmpty();
            assertThat(jobsOnQueue).isEmpty();
            assertThat(commitRequestsOnQueue).containsExactly(
                    commitRequestFor(job1, "test-job-run-1",
                            new RecordsProcessedSummary(job1Summary,
                                    Instant.parse("2024-02-22T13:50:01Z"),
                                    Instant.parse("2024-02-22T13:50:02Z"))),
                    commitRequestFor(job2, "test-job-run-2",
                            new RecordsProcessedSummary(job2Summary,
                                    Instant.parse("2024-02-22T13:50:03Z"),
                                    Instant.parse("2024-02-22T13:50:04Z"))));
            assertThat(jobStore.getAllJobs(table1.get(TABLE_ID))).containsExactly(
                    jobCreated(job1, DEFAULT_CREATED_TIME,
                            startedCompactionRun(DEFAULT_TASK_ID,
                                    Instant.parse("2024-02-22T13:50:01Z"))));
            assertThat(jobStore.getAllJobs(table2.get(TABLE_ID))).containsExactly(
                    jobCreated(job2, DEFAULT_CREATED_TIME,
                            startedCompactionRun(DEFAULT_TASK_ID,
                                    Instant.parse("2024-02-22T13:50:03Z"))));
        }

        @Test
        void shouldOnlySendJobCommitRequestsForTablesConfiguredForAsyncCommit() throws Exception {
            // Given
            table2.set(COMPACTION_JOB_COMMIT_ASYNC, "false");
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"),   // Start
                    Instant.parse("2024-02-22T13:50:01Z"),   // Job 1 started
                    Instant.parse("2024-02-22T13:50:02Z"),   // Job 1 completed
                    Instant.parse("2024-02-22T13:50:03Z"),   // Job 2 started
                    Instant.parse("2024-02-22T13:50:04Z"),   // Job 2 completed
                    Instant.parse("2024-02-22T13:50:07Z"))); // Finish
            CompactionJob job1 = createJobOnQueue("job1", table1, store1);
            RecordsProcessed job1Summary = new RecordsProcessed(10L, 10L);
            CompactionJob job2 = createJobOnQueue("job2", table2, store2);
            RecordsProcessed job2Summary = new RecordsProcessed(20L, 20L);

            // When
            runTask(processJobs(
                    jobSucceeds(job1Summary),
                    jobSucceeds(job2Summary)),
                    times::poll, tablePropertiesProvider, stateStoreProvider);

            // Then
            assertThat(successfulJobs).containsExactly(job1, job2);
            assertThat(failedJobs).isEmpty();
            assertThat(jobsOnQueue).isEmpty();
            assertThat(commitRequestsOnQueue).containsExactly(
                    commitRequestFor(job1,
                            new RecordsProcessedSummary(job1Summary,
                                    Instant.parse("2024-02-22T13:50:01Z"),
                                    Instant.parse("2024-02-22T13:50:02Z"))));
            assertThat(jobStore.getAllJobs(table1.get(TABLE_ID))).containsExactly(
                    jobCreated(job1, DEFAULT_CREATED_TIME,
                            startedCompactionRun(DEFAULT_TASK_ID,
                                    Instant.parse("2024-02-22T13:50:01Z"))));
            assertThat(jobStore.getAllJobs(table2.get(TABLE_ID))).containsExactly(
                    jobCreated(job2, DEFAULT_CREATED_TIME,
                            finishedCompactionRun(DEFAULT_TASK_ID, new RecordsProcessedSummary(job2Summary,
                                    Instant.parse("2024-02-22T13:50:03Z"),
                                    Instant.parse("2024-02-22T13:50:04Z")))));
        }

        private CompactionJobCommitRequest commitRequestFor(CompactionJob job, RecordsProcessedSummary summary) {
            return commitRequestFor(job, "test-job-run-1", summary);
        }

        private CompactionJobCommitRequest commitRequestFor(CompactionJob job, String runId, RecordsProcessedSummary summary) {
            return new CompactionJobCommitRequest(job, DEFAULT_TASK_ID, runId, summary);
        }
    }

    @Nested
    @DisplayName("Update status stores")
    class UpdateStatusStores {
        @BeforeEach
        void setup() {
            tableProperties.set(COMPACTION_JOB_COMMIT_ASYNC, "false");
        }

        @Test
        void shouldSaveTaskAndJobWhenOneJobSucceeds() throws Exception {
            // Given
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job started
                    Instant.parse("2024-02-22T13:50:02Z"), // Job completed
                    Instant.parse("2024-02-22T13:50:05Z"))); // Finish
            CompactionJob job = createJobOnQueue("job1");

            // When
            RecordsProcessed recordsProcessed = new RecordsProcessed(10L, 10L);
            runTask("test-task-1", processJobs(
                    jobSucceeds(recordsProcessed)),
                    times::poll);

            // Then
            RecordsProcessedSummary jobSummary = new RecordsProcessedSummary(recordsProcessed,
                    Instant.parse("2024-02-22T13:50:01Z"),
                    Instant.parse("2024-02-22T13:50:02Z"));
            assertThat(taskStore.getAllTasks()).containsExactly(
                    finishedCompactionTask("test-task-1",
                            Instant.parse("2024-02-22T13:50:00Z"),
                            Instant.parse("2024-02-22T13:50:05Z"),
                            jobSummary));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                    jobCreated(job, DEFAULT_CREATED_TIME,
                            finishedCompactionRun("test-task-1", jobSummary)));
        }

        @Test
        void shouldSaveTaskAndJobsWhenMultipleJobsSucceed() throws Exception {
            // Given
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job 1 started
                    Instant.parse("2024-02-22T13:50:02Z"), // Job 1 completed
                    Instant.parse("2024-02-22T13:50:03Z"), // Job 2 started
                    Instant.parse("2024-02-22T13:50:04Z"), // Job 2 completed
                    Instant.parse("2024-02-22T13:50:05Z"))); // Finish
            CompactionJob job1 = createJobOnQueue("job1");
            CompactionJob job2 = createJobOnQueue("job2");

            // When
            RecordsProcessed job1RecordsProcessed = new RecordsProcessed(10L, 10L);
            RecordsProcessed job2RecordsProcessed = new RecordsProcessed(5L, 5L);
            runTask("test-task-1", processJobs(
                    jobSucceeds(job1RecordsProcessed),
                    jobSucceeds(job2RecordsProcessed)),
                    times::poll);

            // Then
            RecordsProcessedSummary job1Summary = new RecordsProcessedSummary(job1RecordsProcessed,
                    Instant.parse("2024-02-22T13:50:01Z"),
                    Instant.parse("2024-02-22T13:50:02Z"));
            RecordsProcessedSummary job2Summary = new RecordsProcessedSummary(job2RecordsProcessed,
                    Instant.parse("2024-02-22T13:50:03Z"),
                    Instant.parse("2024-02-22T13:50:04Z"));
            assertThat(taskStore.getAllTasks()).containsExactly(
                    finishedCompactionTask("test-task-1",
                            Instant.parse("2024-02-22T13:50:00Z"),
                            Instant.parse("2024-02-22T13:50:05Z"),
                            job1Summary, job2Summary));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactlyInAnyOrder(
                    jobCreated(job1, DEFAULT_CREATED_TIME,
                            finishedCompactionRun("test-task-1", job1Summary)),
                    jobCreated(job2, DEFAULT_CREATED_TIME,
                            finishedCompactionRun("test-task-1", job2Summary)));
        }

        @Test
        void shouldSaveTaskAndJobWhenOneJobFails() throws Exception {
            // Given
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job start
                    Instant.parse("2024-02-22T13:50:05Z"), // Job failed
                    Instant.parse("2024-02-22T13:50:06Z"))); // Task finish
            CompactionJob job = createJobOnQueue("job1");
            RuntimeException root = new RuntimeException("Root failure");
            RuntimeException cause = new RuntimeException("Details of cause", root);
            RuntimeException failure = new RuntimeException("Something went wrong", cause);

            // When
            runTask("test-task-1", processJobs(jobFails(failure)), times::poll);

            // Then
            assertThat(taskStore.getAllTasks()).containsExactly(
                    finishedCompactionTask("test-task-1",
                            Instant.parse("2024-02-22T13:50:00Z"),
                            Instant.parse("2024-02-22T13:50:06Z")));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                    jobCreated(job, DEFAULT_CREATED_TIME,
                            failedCompactionRun("test-task-1",
                                    new ProcessRunTime(
                                            Instant.parse("2024-02-22T13:50:01Z"),
                                            Instant.parse("2024-02-22T13:50:05Z")),
                                    List.of("Something went wrong", "Details of cause", "Root failure"))));
        }

        @Test
        void shouldSaveTaskWhenNoJobsFound() throws Exception {
            // Given
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:05Z"))); // Finish

            // When
            runTask("test-task-1", processNoJobs(), times::poll);

            // Then
            assertThat(taskStore.getAllTasks()).containsExactly(
                    finishedCompactionTask("test-task-1",
                            Instant.parse("2024-02-22T13:50:00Z"),
                            Instant.parse("2024-02-22T13:50:05Z")));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).isEmpty();
        }

        @Test
        void shouldSaveAsynchronousCommitsAfterTwoRunsFinished() throws Exception {
            // Given
            Instant startTime1 = Instant.parse("2024-02-22T13:50:01Z");
            Instant finishTime1 = Instant.parse("2024-02-22T13:50:02Z");
            Instant startTime2 = Instant.parse("2024-02-22T13:50:03Z");
            Instant finishTime2 = Instant.parse("2024-02-22T13:50:04Z");
            Queue<Instant> timesInTask = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    startTime1, finishTime1, startTime2, finishTime2,
                    Instant.parse("2024-02-22T13:50:05Z"))); // Finish
            Queue<String> jobRunIds = new LinkedList<>(List.of(
                    "test-job-run-1", "test-job-run-2"));
            CompactionJob job = createJob("test-job");
            send(job);
            send(job);
            tableProperties.set(COMPACTION_JOB_COMMIT_ASYNC, "true");

            // When a task runs
            RecordsProcessed recordsProcessed = new RecordsProcessed(10L, 10L);
            runTask("test-task", processJobs(
                    jobSucceeds(recordsProcessed),
                    jobSucceeds(recordsProcessed)),
                    jobRunIds::poll, timesInTask::poll);
            // And the commits are saved to the status store
            jobStore.jobFinished(compactionJobFinished(job, summary(startTime1, finishTime1, 10, 10))
                    .taskId("test-task").jobRunId("test-job-run-1").build());
            jobStore.jobFailed(compactionJobFailed(job, new ProcessRunTime(startTime2, finishTime2))
                    .failureReasons(List.of("Could not commit same job twice"))
                    .taskId("test-task").jobRunId("test-job-run-2").build());

            // Then
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                    jobCreated(job, DEFAULT_CREATED_TIME,
                            ProcessRun.builder().taskId("test-task")
                                    .startedStatus(compactionStartedStatus(startTime2))
                                    .finishedStatus(compactionFailedStatus(new ProcessRunTime(startTime2, finishTime2), List.of("Could not commit same job twice")))
                                    .build(),
                            ProcessRun.builder().taskId("test-task")
                                    .startedStatus(compactionStartedStatus(startTime1))
                                    .finishedStatus(compactionFinishedStatus(summary(startTime1, finishTime1, 10, 10)))
                                    .build()));
        }

        private CompactionTaskFinishedStatus.Builder withJobSummaries(RecordsProcessedSummary... summaries) {
            CompactionTaskFinishedStatus.Builder taskFinishedBuilder = CompactionTaskFinishedStatus.builder();
            Stream.of(summaries).forEach(taskFinishedBuilder::addJobSummary);
            return taskFinishedBuilder;
        }

        private CompactionTaskStatus finishedCompactionTask(String taskId, Instant startTime, Instant finishTime, RecordsProcessedSummary... summaries) {
            return CompactionTaskStatus.builder()
                    .startTime(startTime)
                    .taskId(taskId)
                    .finished(finishTime, withJobSummaries(summaries))
                    .build();
        }
    }
}
