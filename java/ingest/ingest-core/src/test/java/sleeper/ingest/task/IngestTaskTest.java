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
package sleeper.ingest.task;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.ingest.IngestResult;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobHandler;
import sleeper.ingest.job.commit.IngestJobCommitRequest;
import sleeper.ingest.job.commit.IngestJobCommitter;
import sleeper.ingest.job.status.InMemoryIngestJobStatusStore;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.task.IngestTask.MessageHandle;
import sleeper.ingest.task.IngestTask.MessageReceiver;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;
import static sleeper.ingest.IngestResultTestData.defaultFileIngestResult;
import static sleeper.ingest.IngestResultTestData.defaultFileIngestResultReadAndWritten;
import static sleeper.ingest.job.IngestJobTestData.DEFAULT_TABLE_ID;
import static sleeper.ingest.job.status.IngestJobStatusTestData.failedIngestJob;
import static sleeper.ingest.job.status.IngestJobStatusTestData.finishedIngestJob;
import static sleeper.ingest.job.status.IngestJobStatusTestData.startedIngestJob;
import static sleeper.ingest.task.IngestTaskStatusTestData.finishedMultipleJobs;
import static sleeper.ingest.task.IngestTaskStatusTestData.finishedNoJobs;
import static sleeper.ingest.task.IngestTaskStatusTestData.finishedOneJob;

public class IngestTaskTest {
    private static final String DEFAULT_TASK_ID = "test-task-id";

    private final Queue<IngestJob> jobsOnQueue = new LinkedList<>();
    private final List<IngestJob> successfulJobs = new ArrayList<>();
    private final List<IngestJob> failedJobs = new ArrayList<>();
    private final List<IngestJobCommitRequest> asyncCommitRequests = new ArrayList<>();
    private final IngestJobStatusStore jobStore = new InMemoryIngestJobStatusStore();
    private final IngestTaskStatusStore taskStore = new InMemoryIngestTaskStatusStore();

    @Nested
    @DisplayName("Process jobs")
    class ProcessJobs {
        @Test
        void shouldRunJobFromQueueThenTerminate() throws Exception {
            // Given
            IngestJob job = createJobOnQueue("job1");

            // When
            runTask(jobsSucceed(1));

            // Then
            assertThat(successfulJobs).containsExactly(job);
            assertThat(failedJobs).isEmpty();
            assertThat(jobsOnQueue).isEmpty();
        }

        @Test
        void shouldFailJobFromQueueThenTerminate() throws Exception {
            // Given
            IngestJob job = createJobOnQueue("job1");

            // When
            runTask(processJobs(jobFails()));

            // Then
            assertThat(successfulJobs).isEmpty();
            assertThat(failedJobs).containsExactly(job);
            assertThat(jobsOnQueue).isEmpty();
        }

        @Test
        void shouldProcessTwoJobsFromQueueThenTerminate() throws Exception {
            // Given
            IngestJob job1 = createJobOnQueue("job1");
            IngestJob job2 = createJobOnQueue("job2");

            // When
            runTask(processJobs(jobSucceeds(), jobFails()));

            // Then
            assertThat(successfulJobs).containsExactly(job1);
            assertThat(failedJobs).containsExactly(job2);
            assertThat(jobsOnQueue).isEmpty();
        }

        @Test
        void shouldTerminateTaskEarlyWhenJobFails() throws Exception {
            // Given
            IngestJob job1 = createJobOnQueue("job1");
            IngestJob job2 = createJobOnQueue("job2");
            IngestJob job3 = createJobOnQueue("job3");

            // When
            runTask(processJobs(
                    jobSucceeds(),
                    jobFails(),
                    jobSucceeds()));

            // Then
            assertThat(successfulJobs).containsExactly(job1);
            assertThat(failedJobs).containsExactly(job2);
            assertThat(jobsOnQueue).containsExactly(job3);
        }
    }

    @Nested
    @DisplayName("Update status stores")
    class UpdateStatusStores {
        @Test
        void shouldSaveTaskAndJobWhenOneJobSucceeds() throws Exception {
            // Given
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job start
                    Instant.parse("2024-02-22T13:50:02Z"), // Job finish
                    Instant.parse("2024-02-22T13:50:05Z"))); // Finish
            IngestJob job = createJobOnQueue("job1");

            // When
            IngestResult jobResult = recordsReadAndWritten(10L, 10L);
            runTask("test-task-1", processJobs(
                    jobSucceeds(jobResult)),
                    times::poll);

            // Then
            assertThat(taskStore.getAllTasks()).containsExactly(
                    finishedOneJob("test-task-1",
                            Instant.parse("2024-02-22T13:50:00Z"), Instant.parse("2024-02-22T13:50:05Z"),
                            Instant.parse("2024-02-22T13:50:01Z"), Instant.parse("2024-02-22T13:50:02Z"), 10L, 10L));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                    finishedIngestJob(job, "test-task-1", summary(jobResult,
                            Instant.parse("2024-02-22T13:50:01Z"),
                            Instant.parse("2024-02-22T13:50:02Z"))));
        }

        @Test
        void shouldSaveTaskAndJobWhenOneJobSucceedsWithDifferentReadAndWrittenCounts() throws Exception {
            // Given
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job start
                    Instant.parse("2024-02-22T13:50:02Z"), // Job finish
                    Instant.parse("2024-02-22T13:50:05Z"))); // Finish
            IngestJob job = createJobOnQueue("job1");

            // When
            IngestResult jobResult = recordsReadAndWritten(10L, 5L);
            runTask("test-task-1", processJobs(
                    jobSucceeds(jobResult)),
                    times::poll);

            // Then
            assertThat(taskStore.getAllTasks()).containsExactly(
                    finishedOneJob("test-task-1",
                            Instant.parse("2024-02-22T13:50:00Z"), Instant.parse("2024-02-22T13:50:05Z"),
                            Instant.parse("2024-02-22T13:50:01Z"), Instant.parse("2024-02-22T13:50:02Z"), 10L, 5L));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                    finishedIngestJob(job, "test-task-1", summary(jobResult,
                            Instant.parse("2024-02-22T13:50:01Z"),
                            Instant.parse("2024-02-22T13:50:02Z"))));
        }

        @Test
        void shouldSaveTaskAndJobWhenMultipleJobsSucceed() throws Exception {
            // Given
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job 1 start
                    Instant.parse("2024-02-22T13:50:02Z"), // Job 1 finish
                    Instant.parse("2024-02-22T13:50:03Z"), // Job 2 start
                    Instant.parse("2024-02-22T13:50:04Z"), // Job 2 finish
                    Instant.parse("2024-02-22T13:50:05Z"))); // Finish
            IngestJob job1 = createJobOnQueue("job1");
            IngestJob job2 = createJobOnQueue("job2");

            // When
            IngestResult job1Result = recordsReadAndWritten(10L, 10L);
            IngestResult job2Result = recordsReadAndWritten(5L, 5L);
            runTask("test-task-1", processJobs(
                    jobSucceeds(job1Result),
                    jobSucceeds(job2Result)),
                    times::poll);

            // Then
            assertThat(taskStore.getAllTasks()).containsExactly(
                    finishedMultipleJobs("test-task-1",
                            Instant.parse("2024-02-22T13:50:00Z"),
                            Instant.parse("2024-02-22T13:50:05Z"),
                            summary(job1Result,
                                    Instant.parse("2024-02-22T13:50:01Z"),
                                    Instant.parse("2024-02-22T13:50:02Z")),
                            summary(job2Result,
                                    Instant.parse("2024-02-22T13:50:03Z"),
                                    Instant.parse("2024-02-22T13:50:04Z"))));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactlyInAnyOrder(
                    finishedIngestJob(job1, "test-task-1", summary(job1Result,
                            Instant.parse("2024-02-22T13:50:01Z"),
                            Instant.parse("2024-02-22T13:50:02Z"))),
                    finishedIngestJob(job2, "test-task-1", summary(job2Result,
                            Instant.parse("2024-02-22T13:50:03Z"),
                            Instant.parse("2024-02-22T13:50:04Z"))));
        }

        @Test
        void shouldSaveTaskWhenOnlyJobFails() throws Exception {
            // Given
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job start
                    Instant.parse("2024-02-22T13:50:05Z"), // Job failed
                    Instant.parse("2024-02-22T13:50:06Z"))); // Task finish
            IngestJob job = createJobOnQueue("job1");
            RuntimeException root = new RuntimeException("Root cause details");
            RuntimeException cause = new RuntimeException("Failure cause details", root);
            RuntimeException failure = new RuntimeException("Something went wrong", cause);

            // When
            runTask("test-task-1", processJobs(jobFails(failure)), times::poll);

            // Then
            assertThat(taskStore.getAllTasks()).containsExactly(
                    finishedNoJobs("test-task-1",
                            Instant.parse("2024-02-22T13:50:00Z"),
                            Instant.parse("2024-02-22T13:50:06Z")));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                    failedIngestJob(job, "test-task-1",
                            new ProcessRunTime(
                                    Instant.parse("2024-02-22T13:50:01Z"),
                                    Instant.parse("2024-02-22T13:50:05Z")),
                            List.of("Something went wrong", "Failure cause details", "Root cause details")));
        }

        @Test
        void shouldSaveTaskAndJobWhenSecondJobFails() throws Exception {
            // Given
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job 1 start
                    Instant.parse("2024-02-22T13:50:02Z"), // Job 1 finish
                    Instant.parse("2024-02-22T13:50:03Z"), // Job 2 start
                    Instant.parse("2024-02-22T13:50:05Z"), // Job 2 failed
                    Instant.parse("2024-02-22T13:50:06Z"))); // Task finish
            IngestJob job1 = createJobOnQueue("job1");
            IngestJob job2 = createJobOnQueue("job2");
            RuntimeException failure = new RuntimeException("Something went wrong");

            // When
            IngestResult job1Result = recordsReadAndWritten(10L, 10L);
            runTask("test-task-1", processJobs(
                    jobSucceeds(job1Result),
                    jobFails(failure)),
                    times::poll);

            // Then
            assertThat(taskStore.getAllTasks()).containsExactly(
                    finishedOneJob("test-task-1",
                            Instant.parse("2024-02-22T13:50:00Z"), Instant.parse("2024-02-22T13:50:06Z"),
                            Instant.parse("2024-02-22T13:50:01Z"), Instant.parse("2024-02-22T13:50:02Z"), 10L, 10L));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactlyInAnyOrder(
                    finishedIngestJob(job1, "test-task-1", summary(job1Result,
                            Instant.parse("2024-02-22T13:50:01Z"),
                            Instant.parse("2024-02-22T13:50:02Z"))),
                    failedIngestJob(job2, "test-task-1",
                            new ProcessRunTime(
                                    Instant.parse("2024-02-22T13:50:03Z"),
                                    Instant.parse("2024-02-22T13:50:05Z")),
                            List.of("Something went wrong")));
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
                    finishedNoJobs("test-task-1",
                            Instant.parse("2024-02-22T13:50:00Z"),
                            Instant.parse("2024-02-22T13:50:05Z")));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).isEmpty();
        }

        @Test
        void shouldSaveTaskWhenJobWithNoFilesSucceeds() throws Exception {
            // Given
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job start
                    Instant.parse("2024-02-22T13:50:02Z"), // Job finish
                    Instant.parse("2024-02-22T13:50:05Z"))); // Finish
            IngestJob job = createJobOnQueueNoFiles("job1");

            // When
            IngestResult jobResult = IngestResult.noFiles();
            runTask("test-task-1", processJobs(
                    jobSucceeds(jobResult)),
                    times::poll);

            // Then
            assertThat(taskStore.getAllTasks()).containsExactly(
                    finishedOneJob("test-task-1",
                            Instant.parse("2024-02-22T13:50:00Z"), Instant.parse("2024-02-22T13:50:05Z"),
                            Instant.parse("2024-02-22T13:50:01Z"), Instant.parse("2024-02-22T13:50:02Z"), 0L, 0L));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                    finishedIngestJob(job, "test-task-1", summary(jobResult,
                            Instant.parse("2024-02-22T13:50:01Z"),
                            Instant.parse("2024-02-22T13:50:02Z"))));
        }

        @Test
        void shouldNotUpdateJobStatusStoreIfAsyncCommitsEnabled() throws Exception {
            // Given
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job start
                    Instant.parse("2024-02-22T13:50:02Z"), // Job finish
                    Instant.parse("2024-02-22T13:50:05Z"))); // Finish
            IngestJob job = createJobOnQueue("job1");

            // When
            IngestResult jobResult = recordsReadAndWritten(10L, 10L);
            runTaskWithAsyncCommits("test-task-1", processJobs(
                    jobSucceeds(jobResult)),
                    times::poll);

            // Then
            assertThat(asyncCommitRequests)
                    .containsExactly(new IngestJobCommitRequest(job, "test-task-1", jobResult.getFileReferenceList(),
                            summary(jobResult, Instant.parse("2024-02-22T13:50:01Z"), Instant.parse("2024-02-22T13:50:02Z"))));
            assertThat(taskStore.getAllTasks()).containsExactly(
                    finishedOneJob("test-task-1",
                            Instant.parse("2024-02-22T13:50:00Z"), Instant.parse("2024-02-22T13:50:05Z"),
                            Instant.parse("2024-02-22T13:50:01Z"), Instant.parse("2024-02-22T13:50:02Z"), 10L, 10L));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                    startedIngestJob(job, "test-task-1", Instant.parse("2024-02-22T13:50:01Z")));
        }
    }

    private void runTask(IngestJobHandler ingestRunner) throws Exception {
        runTask(ingestRunner, Instant::now);
    }

    private void runTask(IngestJobHandler ingestRunner, Supplier<Instant> timeSupplier) throws Exception {
        runTask(pollQueue(), ingestRunner, timeSupplier, DEFAULT_TASK_ID, false);
    }

    private void runTask(String taskId, IngestJobHandler ingestRunner, Supplier<Instant> timeSupplier) throws Exception {
        runTask(pollQueue(), ingestRunner, timeSupplier, taskId, false);
    }

    private void runTaskWithAsyncCommits(String taskId, IngestJobHandler ingestRunner, Supplier<Instant> timeSupplier) throws Exception {
        runTask(pollQueue(), ingestRunner, timeSupplier, taskId, true);
    }

    private void runTask(
            MessageReceiver messageReceiver,
            IngestJobHandler ingestRunner,
            Supplier<Instant> timeSupplier,
            String taskId,
            boolean shouldAsyncCommit) throws Exception {
        IngestJobCommitterOrSendToLambda jobCommitterOrSendToLambda = new IngestJobCommitterOrSendToLambda(
                tableId -> shouldAsyncCommit,
                new IngestJobCommitter(jobStore, tableId -> inMemoryStateStoreWithFixedSinglePartition(schemaWithKey("key"))),
                asyncCommitRequests::add);
        new IngestTask(timeSupplier, messageReceiver, ingestRunner, jobCommitterOrSendToLambda,
                jobStore, taskStore, taskId)
                .run();
    }

    private MessageReceiver pollQueue() {
        return () -> {
            IngestJob job = jobsOnQueue.poll();
            if (job != null) {
                return Optional.of(new FakeMessageHandle(job));
            } else {
                return Optional.empty();
            }
        };
    }

    private IngestJob createJobOnQueue(String jobId) {
        IngestJob job = IngestJob.builder()
                .tableId(DEFAULT_TABLE_ID)
                .tableName("test-table")
                .files(UUID.randomUUID().toString())
                .id(jobId)
                .build();
        jobsOnQueue.add(job);
        return job;
    }

    private IngestJob createJobOnQueueNoFiles(String jobId) {
        IngestJob job = IngestJob.builder()
                .tableId(DEFAULT_TABLE_ID)
                .tableName("test-table")
                .files(List.of())
                .id(jobId)
                .build();
        jobsOnQueue.add(job);
        return job;
    }

    private IngestResult recordsReadAndWritten(long recordsRead, long recordsWritten) {
        return defaultFileIngestResultReadAndWritten("test-file", recordsRead, recordsWritten);
    }

    private RecordsProcessedSummary summary(IngestResult result, Instant startTime, Instant finishTime) {
        return new RecordsProcessedSummary(result.asRecordsProcessed(), startTime, finishTime);
    }

    private IngestJobHandler jobsSucceed(int numJobs) {
        return processJobs(Stream.generate(() -> jobSucceeds())
                .limit(numJobs)
                .toArray(ProcessJob[]::new));
    }

    private ProcessJob jobSucceeds(IngestResult result) {
        return new ProcessJob(result);
    }

    private ProcessJob jobSucceeds() {
        return new ProcessJob(defaultFileIngestResult("test-file"));
    }

    private ProcessJob jobFails() {
        return new ProcessJob(new RuntimeException("Failed to process job"));
    }

    private ProcessJob jobFails(RuntimeException e) {
        return new ProcessJob(e);
    }

    private IngestJobHandler processNoJobs() {
        return processJobs();
    }

    private IngestJobHandler processJobs(ProcessJob... actions) {
        Iterator<ProcessJob> getAction = List.of(actions).iterator();
        return job -> {
            if (getAction.hasNext()) {
                ProcessJob action = getAction.next();
                if (action.failure != null) {
                    throw action.failure;
                } else {
                    successfulJobs.add(job);
                    return action.result;
                }
            } else {
                throw new IllegalStateException("Unexpected job: " + job);
            }
        };
    }

    private class ProcessJob {
        private final RuntimeException failure;
        private final IngestResult result;

        ProcessJob(RuntimeException failure) {
            this.failure = failure;
            this.result = null;
        }

        ProcessJob(IngestResult result) {
            this.failure = null;
            this.result = result;
        }
    }

    private class FakeMessageHandle implements MessageHandle {
        private final IngestJob job;

        FakeMessageHandle(IngestJob job) {
            this.job = job;
        }

        public IngestJob getJob() {
            return job;
        }

        public void close() {
        }

        public void completed(RecordsProcessedSummary summary) {
        }

        public void failed() {
            failedJobs.add(job);
        }
    }
}
