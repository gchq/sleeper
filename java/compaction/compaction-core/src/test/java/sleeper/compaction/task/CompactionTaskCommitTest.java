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
package sleeper.compaction.task;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;

import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionCommittedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionFailedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionFinishedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.failedCompactionRun;
import static sleeper.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.status.CompactionJobCommittedEvent.compactionJobCommitted;
import static sleeper.compaction.job.status.CompactionJobFailedEvent.compactionJobFailed;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_WAIT_FOR_INPUT_FILE_ASSIGNMENT;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_COMMIT_ASYNC;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;

public class CompactionTaskCommitTest extends CompactionTaskTestBase {
    private final TableProperties table1 = createTable("test-table-1-id", "test-table-1");
    private final TableProperties table2 = createTable("test-table-2-id", "test-table-2");
    private final StateStore store1 = stateStore(table1);
    private final StateStore store2 = stateStore(table2);

    @Nested
    @DisplayName("Send commits to queue")
    class SendCommitsToQueue {

        @Test
        void shouldSendJobCommitRequestToQueue() throws Exception {
            // Given
            setAsyncCommit(true, tableProperties);
            Instant startTime = Instant.parse("2024-02-22T13:50:01Z");
            Instant finishTime = Instant.parse("2024-02-22T13:50:02Z");
            Iterator<Instant> times = List.of(
                    Instant.parse("2024-02-22T13:50:00Z"),   // Task start
                    startTime, finishTime,
                    Instant.parse("2024-02-22T13:50:05Z")).iterator(); // Task finish
            CompactionJob job1 = createJobOnQueue("job1");
            RecordsProcessed job1Summary = new RecordsProcessed(10L, 5L);

            // When
            runTask(processJobs(jobSucceeds(job1Summary)), times::next);

            // Then
            assertThat(consumedJobs).containsExactly(job1);
            assertThat(jobsReturnedToQueue).isEmpty();
            assertThat(jobsOnQueue).isEmpty();
            assertThat(commitRequestsOnQueue).containsExactly(
                    commitRequestFor(job1,
                            new RecordsProcessedSummary(job1Summary,
                                    Instant.parse("2024-02-22T13:50:01Z"),
                                    Instant.parse("2024-02-22T13:50:02Z"))));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                    jobCreated(job1, DEFAULT_CREATED_TIME,
                            ProcessRun.builder().taskId(DEFAULT_TASK_ID)
                                    .startedStatus(compactionStartedStatus(startTime))
                                    .finishedStatus(compactionFinishedStatus(summary(startTime, finishTime, 10, 5)))
                                    .build()));
        }

        @Test
        void shouldSendJobCommitRequestsForDifferentTablesToQueue() throws Exception {
            // Given
            setAsyncCommit(true, table1, table2);
            Instant startTime1 = Instant.parse("2024-02-22T13:50:01Z");
            Instant finishTime1 = Instant.parse("2024-02-22T13:50:02Z");
            Instant startTime2 = Instant.parse("2024-02-22T13:50:03Z");
            Instant finishTime2 = Instant.parse("2024-02-22T13:50:04Z");
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"),   // Task start
                    startTime1, finishTime1, startTime2, finishTime2,
                    Instant.parse("2024-02-22T13:50:07Z"))); // Task finish
            CompactionJob job1 = createJobOnQueue("job1", table1, store1);
            RecordsProcessed job1Records = new RecordsProcessed(10L, 10L);
            CompactionJob job2 = createJobOnQueue("job2", table2, store2);
            RecordsProcessed job2Records = new RecordsProcessed(20L, 20L);

            // When
            runTask(processJobs(
                    jobSucceeds(job1Records),
                    jobSucceeds(job2Records)),
                    times::poll);

            // Then
            assertThat(consumedJobs).containsExactly(job1, job2);
            assertThat(jobsReturnedToQueue).isEmpty();
            assertThat(jobsOnQueue).isEmpty();
            assertThat(commitRequestsOnQueue).containsExactly(
                    commitRequestFor(job1, "test-job-run-1",
                            new RecordsProcessedSummary(job1Records, startTime1, finishTime1)),
                    commitRequestFor(job2, "test-job-run-2",
                            new RecordsProcessedSummary(job2Records, startTime2, finishTime2)));
            assertThat(jobStore.getAllJobs(table1.get(TABLE_ID))).containsExactly(
                    jobCreated(job1, DEFAULT_CREATED_TIME,
                            ProcessRun.builder().taskId(DEFAULT_TASK_ID)
                                    .startedStatus(compactionStartedStatus(startTime1))
                                    .finishedStatus(compactionFinishedStatus(
                                            new RecordsProcessedSummary(job1Records, startTime1, finishTime1)))
                                    .build()));
            assertThat(jobStore.getAllJobs(table2.get(TABLE_ID))).containsExactly(
                    jobCreated(job2, DEFAULT_CREATED_TIME,
                            ProcessRun.builder().taskId(DEFAULT_TASK_ID)
                                    .startedStatus(compactionStartedStatus(startTime2))
                                    .finishedStatus(compactionFinishedStatus(
                                            new RecordsProcessedSummary(job2Records, startTime2, finishTime2)))
                                    .build()));
        }

        @Test
        void shouldOnlySendJobCommitRequestsForTablesConfiguredForAsyncCommit() throws Exception {
            // Given
            setAsyncCommit(true, table1);
            setAsyncCommit(false, table2);
            Instant startTime1 = Instant.parse("2024-02-22T13:50:01Z");
            Instant finishTime1 = Instant.parse("2024-02-22T13:50:02Z");
            Instant startTime2 = Instant.parse("2024-02-22T13:50:03Z");
            Instant finishTime2 = Instant.parse("2024-02-22T13:50:04Z");
            Instant commitTime2 = Instant.parse("2024-02-22T13:50:05Z");
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"),   // Task start
                    startTime1, finishTime1, startTime2, finishTime2, commitTime2,
                    Instant.parse("2024-02-22T13:50:07Z"))); // Task finish
            CompactionJob job1 = createJobOnQueue("job1", table1, store1);
            RecordsProcessed job1Records = new RecordsProcessed(10L, 10L);
            CompactionJob job2 = createJobOnQueue("job2", table2, store2);
            RecordsProcessed job2Records = new RecordsProcessed(20L, 20L);

            // When
            runTask(processJobs(
                    jobSucceeds(job1Records),
                    jobSucceeds(job2Records)),
                    times::poll);

            // Then
            assertThat(consumedJobs).containsExactly(job1, job2);
            assertThat(jobsReturnedToQueue).isEmpty();
            assertThat(jobsOnQueue).isEmpty();
            assertThat(commitRequestsOnQueue).containsExactly(
                    commitRequestFor(job1, new RecordsProcessedSummary(job1Records, startTime1, finishTime1)));
            assertThat(jobStore.getAllJobs(table1.get(TABLE_ID))).containsExactly(
                    jobCreated(job1, DEFAULT_CREATED_TIME,
                            ProcessRun.builder().taskId(DEFAULT_TASK_ID)
                                    .startedStatus(compactionStartedStatus(startTime1))
                                    .finishedStatus(compactionFinishedStatus(
                                            new RecordsProcessedSummary(job1Records, startTime1, finishTime1)))
                                    .build()));
            assertThat(jobStore.getAllJobs(table2.get(TABLE_ID))).containsExactly(
                    jobCreated(job2, DEFAULT_CREATED_TIME,
                            ProcessRun.builder().taskId(DEFAULT_TASK_ID)
                                    .startedStatus(compactionStartedStatus(startTime2))
                                    .finishedStatus(compactionFinishedStatus(
                                            new RecordsProcessedSummary(job2Records, startTime2, finishTime2)))
                                    .statusUpdate(compactionCommittedStatus(commitTime2))
                                    .build()));
        }

        @Test
        void shouldCorrelateAsynchronousCommitsAfterTwoRunsFinished() throws Exception {
            // Given
            setAsyncCommit(true, tableProperties);
            Instant startTime1 = Instant.parse("2024-02-22T13:50:01Z");
            Instant finishTime1 = Instant.parse("2024-02-22T13:50:02Z");
            Instant startTime2 = Instant.parse("2024-02-22T13:50:03Z");
            Instant finishTime2 = Instant.parse("2024-02-22T13:50:04Z");
            Instant commitTime = Instant.parse("2024-02-22T13:50:10Z");
            Instant commitFailTime = Instant.parse("2024-02-22T13:50:10Z");
            Queue<Instant> timesInTask = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    startTime1, finishTime1, startTime2, finishTime2,
                    Instant.parse("2024-02-22T13:50:05Z"))); // Finish
            Queue<String> jobRunIds = new LinkedList<>(List.of(
                    "test-job-run-1", "test-job-run-2"));
            CompactionJob job = createJob("test-job");
            send(job);
            send(job);

            // When a task runs
            RecordsProcessed recordsProcessed = new RecordsProcessed(10L, 10L);
            runTask("test-task", processJobs(
                    jobSucceeds(recordsProcessed),
                    jobSucceeds(recordsProcessed)),
                    jobRunIds::poll, timesInTask::poll);
            // And the commits are saved to the status store
            jobStore.jobCommitted(compactionJobCommitted(job, commitTime)
                    .taskId("test-task").jobRunId("test-job-run-1").build());
            jobStore.jobFailed(compactionJobFailed(job, new ProcessRunTime(startTime2, commitFailTime))
                    .failureReasons(List.of("Could not commit same job twice"))
                    .taskId("test-task").jobRunId("test-job-run-2").build());

            // Then
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                    jobCreated(job, DEFAULT_CREATED_TIME,
                            ProcessRun.builder().taskId("test-task")
                                    .startedStatus(compactionStartedStatus(startTime2))
                                    .statusUpdate(compactionFinishedStatus(
                                            new RecordsProcessedSummary(recordsProcessed, startTime2, finishTime2)))
                                    .finishedStatus(compactionFailedStatus(
                                            new ProcessRunTime(startTime2, commitFailTime),
                                            List.of("Could not commit same job twice")))
                                    .build(),
                            ProcessRun.builder().taskId("test-task")
                                    .startedStatus(compactionStartedStatus(startTime1))
                                    .finishedStatus(compactionFinishedStatus(
                                            new RecordsProcessedSummary(recordsProcessed, startTime1, finishTime1)))
                                    .statusUpdate(compactionCommittedStatus(commitTime))
                                    .build()));
        }

        private CompactionJobCommitRequest commitRequestFor(CompactionJob job, RecordsProcessedSummary summary) {
            return commitRequestFor(job, "test-job-run-1", summary);
        }

        private CompactionJobCommitRequest commitRequestFor(CompactionJob job, String runId, RecordsProcessedSummary summary) {
            return new CompactionJobCommitRequest(job, DEFAULT_TASK_ID, runId, summary);
        }
    }

    @Nested
    @DisplayName("Update state store")
    class UpdateStateStore {
        @BeforeEach
        void setup() {
            tableProperties.set(COMPACTION_JOB_COMMIT_ASYNC, "false");
            table1.set(COMPACTION_JOB_COMMIT_ASYNC, "false");
            table2.set(COMPACTION_JOB_COMMIT_ASYNC, "false");
        }

        @Test
        void shouldCommitCompactionJobsOnDifferentTables() throws Exception {
            // Given
            Instant startTime1 = Instant.parse("2024-02-22T13:50:01Z");
            Instant finishTime1 = Instant.parse("2024-02-22T13:50:02Z");
            Instant commitTime1 = Instant.parse("2024-02-22T13:50:03Z");
            Instant startTime2 = Instant.parse("2024-02-22T13:50:04Z");
            Instant finishTime2 = Instant.parse("2024-02-22T13:50:05Z");
            Instant commitTime2 = Instant.parse("2024-02-22T13:50:06Z");
            Queue<Instant> timesInTask = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    startTime1, finishTime1, commitTime1,
                    startTime2, finishTime2, commitTime2,
                    Instant.parse("2024-02-22T13:50:05Z"))); // Finish
            CompactionJob job1 = createJobOnQueue("job1", table1, store1);
            CompactionJob job2 = createJobOnQueue("job2", table2, store2);
            store1.fixFileUpdateTime(finishTime1);
            store2.fixFileUpdateTime(finishTime2);

            // When
            RecordsProcessed recordsProcessed = new RecordsProcessed(10L, 10L);
            runTask("test-task", processJobs(
                    jobSucceeds(recordsProcessed),
                    jobSucceeds(recordsProcessed)),
                    timesInTask::poll);

            // Then
            assertThat(jobStore.getAllJobs(table1.get(TABLE_ID))).containsExactly(
                    jobCreated(job1, DEFAULT_CREATED_TIME,
                            ProcessRun.builder().taskId("test-task")
                                    .startedStatus(compactionStartedStatus(startTime1))
                                    .finishedStatus(compactionFinishedStatus(
                                            new RecordsProcessedSummary(recordsProcessed, startTime1, finishTime1)))
                                    .statusUpdate(compactionCommittedStatus(commitTime1))
                                    .build()));
            assertThat(jobStore.getAllJobs(table2.get(TABLE_ID))).containsExactly(
                    jobCreated(job2, DEFAULT_CREATED_TIME,
                            ProcessRun.builder().taskId("test-task")
                                    .startedStatus(compactionStartedStatus(startTime2))
                                    .finishedStatus(compactionFinishedStatus(
                                            new RecordsProcessedSummary(recordsProcessed, startTime2, finishTime2)))
                                    .statusUpdate(compactionCommittedStatus(commitTime2))
                                    .build()));
            assertThat(store1.getFileReferences()).containsExactly(
                    FileReferenceFactory.fromUpdatedAt(store1, finishTime1)
                            .rootFile(job1.getOutputFile(), 10));
            assertThat(store2.getFileReferences()).containsExactly(
                    FileReferenceFactory.fromUpdatedAt(store2, finishTime2)
                            .rootFile(job2.getOutputFile(), 10));
        }

        @Test
        void shouldFailWhenFileDeletedDuringJob() throws Exception {
            // Given
            Instant startTime = Instant.parse("2024-02-22T13:50:01Z");
            Instant finishTime = Instant.parse("2024-02-22T13:50:02Z");
            Instant failTime = Instant.parse("2024-02-22T13:50:03Z");
            Queue<Instant> timesInTask = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    startTime, finishTime, failTime,
                    Instant.parse("2024-02-22T13:50:04Z"))); // Finish
            CompactionJob job = createJob("test-job");
            send(job);

            // When
            runTask("test-task", processJobs(jobSucceeds().withAction(() -> {
                stateStore.clearFileData();
            })), timesInTask::poll);

            // Then
            assertThat(stateStore.getFileReferences()).isEmpty();
            assertThat(consumedJobs).containsExactly(job);
            assertThat(jobsReturnedToQueue).isEmpty();
            assertThat(jobStore.getAllJobs(tableProperties.get(TABLE_ID))).containsExactly(
                    jobCreated(job, DEFAULT_CREATED_TIME,
                            failedCompactionRun("test-task", startTime, finishTime, failTime, List.of(
                                    "1 replace file reference requests failed to update the state store",
                                    "File not found: " + job.getInputFiles().get(0)))));
        }

        @Test
        void shouldFailWhenFileAssignedToAnotherJob() throws Exception {
            // Given
            instanceProperties.set(COMPACTION_TASK_WAIT_FOR_INPUT_FILE_ASSIGNMENT, "false");
            Instant startTime = Instant.parse("2024-02-22T13:50:01Z");
            Instant finishTime = Instant.parse("2024-02-22T13:50:02Z");
            Instant failTime = Instant.parse("2024-02-22T13:50:03Z");
            Queue<Instant> timesInTask = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    startTime, finishTime, failTime,
                    Instant.parse("2024-02-22T13:50:04Z"))); // Finish
            CompactionJob job = createJobOnQueueNotAssignedToFiles("test-job");
            stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles("other-job", job.getPartitionId(), job.getInputFiles())));

            // When
            runTask("test-task", processJobs(jobSucceeds()), timesInTask::poll);

            // Then
            assertThat(consumedJobs).containsExactly(job);
            assertThat(jobsReturnedToQueue).isEmpty();
            assertThat(jobStore.getAllJobs(tableProperties.get(TABLE_ID))).containsExactly(
                    jobCreated(job, DEFAULT_CREATED_TIME,
                            failedCompactionRun("test-task", startTime, finishTime, failTime, List.of(
                                    "1 replace file reference requests failed to update the state store",
                                    "Reference to file is not assigned to job test-job, in partition root, filename " + job.getInputFiles().get(0)))));
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
            Instant taskStartTime = Instant.parse("2024-02-22T13:50:00Z");
            Instant jobStartTime = Instant.parse("2024-02-22T13:50:01Z");
            Instant jobFinishTime = Instant.parse("2024-02-22T13:50:02Z");
            Instant jobCommitTime = Instant.parse("2024-02-22T13:50:03Z");
            Instant taskFinishTime = Instant.parse("2024-02-22T13:50:05Z");
            Queue<Instant> times = new LinkedList<>(List.of(
                    taskStartTime,
                    jobStartTime, jobFinishTime, jobCommitTime,
                    taskFinishTime));
            CompactionJob job = createJobOnQueue("job1");

            // When
            RecordsProcessed recordsProcessed = new RecordsProcessed(10L, 10L);
            runTask("test-task-1", processJobs(
                    jobSucceeds(recordsProcessed)),
                    times::poll);

            // Then
            RecordsProcessedSummary jobSummary = new RecordsProcessedSummary(recordsProcessed,
                    jobStartTime, jobFinishTime);
            assertThat(taskStore.getAllTasks()).containsExactly(
                    finishedCompactionTask("test-task-1", taskStartTime, taskFinishTime, jobSummary));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                    jobCreated(job, DEFAULT_CREATED_TIME,
                            finishedCompactionRun("test-task-1", jobSummary, jobCommitTime)));
        }

        @Test
        void shouldSaveTaskAndJobsWhenMultipleJobsSucceed() throws Exception {
            // Given
            Instant taskStartTime = Instant.parse("2024-02-22T13:50:00Z");
            Instant job1StartTime = Instant.parse("2024-02-22T13:50:01Z");
            Instant job1FinishTime = Instant.parse("2024-02-22T13:50:02Z");
            Instant job1CommitTime = Instant.parse("2024-02-22T13:50:03Z");
            Instant job2StartTime = Instant.parse("2024-02-22T13:50:04Z");
            Instant job2FinishTime = Instant.parse("2024-02-22T13:50:05Z");
            Instant job2CommitTime = Instant.parse("2024-02-22T13:50:06Z");
            Instant taskFinishTime = Instant.parse("2024-02-22T13:50:07Z");
            Queue<Instant> times = new LinkedList<>(List.of(
                    taskStartTime,
                    job1StartTime, job1FinishTime, job1CommitTime,
                    job2StartTime, job2FinishTime, job2CommitTime,
                    taskFinishTime));
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
                    job1StartTime, job1FinishTime);
            RecordsProcessedSummary job2Summary = new RecordsProcessedSummary(job2RecordsProcessed,
                    job2StartTime, job2FinishTime);
            assertThat(taskStore.getAllTasks()).containsExactly(
                    finishedCompactionTask("test-task-1", taskStartTime, taskFinishTime, job1Summary, job2Summary));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactlyInAnyOrder(
                    jobCreated(job1, DEFAULT_CREATED_TIME,
                            finishedCompactionRun("test-task-1", job1Summary, job1CommitTime)),
                    jobCreated(job2, DEFAULT_CREATED_TIME,
                            finishedCompactionRun("test-task-1", job2Summary, job2CommitTime)));
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
