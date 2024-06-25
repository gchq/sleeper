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
import sleeper.core.record.process.status.ProcessStatusUpdateRecord;
import sleeper.ingest.IngestResult;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobHandler;
import sleeper.ingest.job.status.InMemoryIngestJobStatusStore;
import sleeper.ingest.job.status.IngestJobUpdateType;
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
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.ingest.IngestResultTestData.defaultFileIngestResult;
import static sleeper.ingest.IngestResultTestData.defaultFileIngestResultReadAndWritten;
import static sleeper.ingest.job.IngestJobTestData.DEFAULT_TABLE_ID;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.failedIngestJob;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.finishedIngestJobUncommitted;
import static sleeper.ingest.job.status.IngestJobUpdateType.FAILED;
import static sleeper.ingest.job.status.IngestJobUpdateType.FINISHED_WHEN_FILES_COMMITTED;
import static sleeper.ingest.job.status.IngestJobUpdateType.STARTED;
import static sleeper.ingest.task.IngestTaskStatusTestData.finishedMultipleJobs;
import static sleeper.ingest.task.IngestTaskStatusTestData.finishedNoJobs;
import static sleeper.ingest.task.IngestTaskStatusTestData.finishedOneJob;

public class IngestTaskTest {
    private static final String DEFAULT_TASK_ID = "test-task-id";

    private final Queue<IngestJob> jobsOnQueue = new LinkedList<>();
    private final List<IngestJob> successfulJobs = new ArrayList<>();
    private final List<IngestJob> failedJobs = new ArrayList<>();
    private final InMemoryIngestJobStatusStore jobStore = new InMemoryIngestJobStatusStore();
    private final IngestTaskStatusStore taskStore = new InMemoryIngestTaskStatusStore();
    private Supplier<Instant> timeSupplier = Instant::now;
    private Supplier<String> jobRunIdSupplier = () -> UUID.randomUUID().toString();

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
            fixTimes(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job start
                    Instant.parse("2024-02-22T13:50:02Z"), // Job finish
                    Instant.parse("2024-02-22T13:50:05Z")); // Finish
            IngestJob job = createJobOnQueue("job1");

            // When
            IngestResult jobResult = recordsReadAndWritten(10L, 10L);
            runTask("test-task-1", processJobs(
                    jobSucceeds(jobResult)));

            // Then
            assertThat(taskStore.getAllTasks()).containsExactly(
                    finishedOneJob("test-task-1",
                            Instant.parse("2024-02-22T13:50:00Z"), Instant.parse("2024-02-22T13:50:05Z"),
                            Instant.parse("2024-02-22T13:50:01Z"), Instant.parse("2024-02-22T13:50:02Z"), 10L, 10L));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                    finishedIngestJobUncommitted(job, "test-task-1", summary(jobResult,
                            Instant.parse("2024-02-22T13:50:01Z"),
                            Instant.parse("2024-02-22T13:50:02Z"))));
        }

        @Test
        void shouldSaveTaskAndJobWhenOneJobSucceedsWithDifferentReadAndWrittenCounts() throws Exception {
            // Given
            fixTimes(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job start
                    Instant.parse("2024-02-22T13:50:02Z"), // Job finish
                    Instant.parse("2024-02-22T13:50:05Z")); // Finish
            IngestJob job = createJobOnQueue("job1");

            // When
            IngestResult jobResult = recordsReadAndWritten(10L, 5L);
            runTask("test-task-1", processJobs(
                    jobSucceeds(jobResult)));

            // Then
            assertThat(taskStore.getAllTasks()).containsExactly(
                    finishedOneJob("test-task-1",
                            Instant.parse("2024-02-22T13:50:00Z"), Instant.parse("2024-02-22T13:50:05Z"),
                            Instant.parse("2024-02-22T13:50:01Z"), Instant.parse("2024-02-22T13:50:02Z"), 10L, 5L));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                    finishedIngestJobUncommitted(job, "test-task-1", summary(jobResult,
                            Instant.parse("2024-02-22T13:50:01Z"),
                            Instant.parse("2024-02-22T13:50:02Z"))));
        }

        @Test
        void shouldSaveTaskAndJobWhenMultipleJobsSucceed() throws Exception {
            // Given
            fixTimes(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job 1 start
                    Instant.parse("2024-02-22T13:50:02Z"), // Job 1 finish
                    Instant.parse("2024-02-22T13:50:03Z"), // Job 2 start
                    Instant.parse("2024-02-22T13:50:04Z"), // Job 2 finish
                    Instant.parse("2024-02-22T13:50:05Z")); // Finish
            IngestJob job1 = createJobOnQueue("job1");
            IngestJob job2 = createJobOnQueue("job2");

            // When
            IngestResult job1Result = recordsReadAndWritten(10L, 10L);
            IngestResult job2Result = recordsReadAndWritten(5L, 5L);
            runTask("test-task-1", processJobs(
                    jobSucceeds(job1Result),
                    jobSucceeds(job2Result)));

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
                    finishedIngestJobUncommitted(job1, "test-task-1", summary(job1Result,
                            Instant.parse("2024-02-22T13:50:01Z"),
                            Instant.parse("2024-02-22T13:50:02Z"))),
                    finishedIngestJobUncommitted(job2, "test-task-1", summary(job2Result,
                            Instant.parse("2024-02-22T13:50:03Z"),
                            Instant.parse("2024-02-22T13:50:04Z"))));
        }

        @Test
        void shouldSaveTaskWhenOnlyJobFails() throws Exception {
            // Given
            fixTimes(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job start
                    Instant.parse("2024-02-22T13:50:05Z"), // Job failed
                    Instant.parse("2024-02-22T13:50:06Z")); // Task finish
            IngestJob job = createJobOnQueue("job1");
            RuntimeException root = new RuntimeException("Root cause details");
            RuntimeException cause = new RuntimeException("Failure cause details", root);
            RuntimeException failure = new RuntimeException("Something went wrong", cause);

            // When
            runTask("test-task-1", processJobs(jobFails(failure)));

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

            fixTimes(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job 1 start
                    Instant.parse("2024-02-22T13:50:02Z"), // Job 1 finish
                    Instant.parse("2024-02-22T13:50:03Z"), // Job 2 start
                    Instant.parse("2024-02-22T13:50:05Z"), // Job 2 failed
                    Instant.parse("2024-02-22T13:50:06Z")); // Task finish
            IngestJob job1 = createJobOnQueue("job1");
            IngestJob job2 = createJobOnQueue("job2");
            RuntimeException failure = new RuntimeException("Something went wrong");

            // When
            IngestResult job1Result = recordsReadAndWritten(10L, 10L);
            runTask("test-task-1", processJobs(
                    jobSucceeds(job1Result),
                    jobFails(failure)));

            // Then
            assertThat(taskStore.getAllTasks()).containsExactly(
                    finishedOneJob("test-task-1",
                            Instant.parse("2024-02-22T13:50:00Z"), Instant.parse("2024-02-22T13:50:06Z"),
                            Instant.parse("2024-02-22T13:50:01Z"), Instant.parse("2024-02-22T13:50:02Z"), 10L, 10L));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactlyInAnyOrder(
                    finishedIngestJobUncommitted(job1, "test-task-1", summary(job1Result,
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
            fixTimes(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:05Z")); // Finish

            // When
            runTask("test-task-1", processNoJobs());

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
            fixTimes(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job start
                    Instant.parse("2024-02-22T13:50:02Z"), // Job finish
                    Instant.parse("2024-02-22T13:50:05Z")); // Finish
            IngestJob job = createJobOnQueueNoFiles("job1");

            // When
            IngestResult jobResult = IngestResult.noFiles();
            runTask("test-task-1", processJobs(
                    jobSucceeds(jobResult)));

            // Then
            assertThat(taskStore.getAllTasks()).containsExactly(
                    finishedOneJob("test-task-1",
                            Instant.parse("2024-02-22T13:50:00Z"), Instant.parse("2024-02-22T13:50:05Z"),
                            Instant.parse("2024-02-22T13:50:01Z"), Instant.parse("2024-02-22T13:50:02Z"), 0L, 0L));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                    finishedIngestJobUncommitted(job, "test-task-1", summary(jobResult,
                            Instant.parse("2024-02-22T13:50:01Z"),
                            Instant.parse("2024-02-22T13:50:02Z")), 0));
        }

        @Test
        void shouldSetJobRunIdOnStatusStoreRecordsWhenFinished() throws Exception {
            // Given
            fixTimes(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job start
                    Instant.parse("2024-02-22T13:50:02Z"), // Job finish
                    Instant.parse("2024-02-22T13:50:05Z")); // Finish
            fixJobRunIds("test-job-run");
            createJobOnQueue("test-job");

            // When
            runTask("test-task", processJobs(
                    jobSucceeds(recordsReadAndWritten(10L, 10L))));

            // Then
            assertThat(jobStore.streamTableRecords(DEFAULT_TABLE_ID))
                    .extracting(
                            ProcessStatusUpdateRecord::getJobRunId,
                            record -> IngestJobUpdateType.typeOfUpdate(record.getStatusUpdate()))
                    .containsExactly(
                            tuple("test-job-run", STARTED),
                            tuple("test-job-run", FINISHED_WHEN_FILES_COMMITTED));
        }

        @Test
        void shouldSetJobRunIdOnStatusStoreRecordsWhenFailed() throws Exception {
            // Given
            fixTimes(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job start
                    Instant.parse("2024-02-22T13:50:02Z"), // Job finish
                    Instant.parse("2024-02-22T13:50:05Z")); // Finish
            fixJobRunIds("test-job-run");
            createJobOnQueue("test-job");
            RuntimeException failure = new RuntimeException("Something went wrong");

            // When
            runTask("test-task", processJobs(jobFails(failure)));

            // Then
            assertThat(jobStore.streamTableRecords(DEFAULT_TABLE_ID))
                    .extracting(
                            ProcessStatusUpdateRecord::getJobRunId,
                            record -> IngestJobUpdateType.typeOfUpdate(record.getStatusUpdate()))
                    .containsExactly(
                            tuple("test-job-run", STARTED),
                            tuple("test-job-run", FAILED));
        }
    }

    private void runTask(IngestJobHandler ingestRunner) throws Exception {
        runTask(DEFAULT_TASK_ID, ingestRunner);
    }

    private void runTask(
            String taskId,
            IngestJobHandler ingestRunner) throws Exception {
        new IngestTask(jobRunIdSupplier, timeSupplier, pollQueue(), ingestRunner, jobStore, taskStore, taskId)
                .run();
    }

    private void fixTimes(Instant... times) {
        timeSupplier = new LinkedList<>(List.of(times))::poll;
    }

    private void fixJobRunIds(String... jobRunIds) {
        jobRunIdSupplier = new LinkedList<>(List.of(jobRunIds))::poll;
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
                .files(List.of(UUID.randomUUID().toString()))
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
        return (job, jobRunId) -> {
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
