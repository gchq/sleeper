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

import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.ingest.IngestResult;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobHandler;
import sleeper.ingest.job.status.InMemoryIngestJobStatusStore;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.task.NewIngestTask.MessageHandle;
import sleeper.ingest.task.NewIngestTask.MessageReceiver;

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
import static sleeper.ingest.IngestResultTestData.defaultFileIngestResult;
import static sleeper.ingest.IngestResultTestData.defaultFileIngestResultReadAndWritten;

public class NewIngestTaskTest {
    private static final String DEFAULT_TASK_ID = "test-task-id";

    private final Queue<IngestJob> jobsOnQueue = new LinkedList<>();
    private final List<IngestJob> successfulJobs = new ArrayList<>();
    private final List<IngestJob> failedJobs = new ArrayList<>();
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
    }

    @Nested
    @DisplayName("Update task status store")
    class UpdateTaskStatusStore {
        @Test
        void shouldSaveTaskWhenOneJobSucceeds() throws Exception {
            // Given
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job start
                    Instant.parse("2024-02-22T13:50:02Z"), // Job finish
                    Instant.parse("2024-02-22T13:50:05Z"))); // Finish
            createJobOnQueue("job1");

            // When
            IngestResult jobResult = recordsReadAndWritten(10L, 10L);
            runTask("test-task-1", processJobs(
                    jobSucceeds(jobResult)),
                    times::poll);

            // Then
            assertThat(taskStore.getAllTasks())
                    .containsExactly(IngestTaskStatus.builder()
                            .startTime(Instant.parse("2024-02-22T13:50:00Z"))
                            .taskId("test-task-1")
                            .finished(Instant.parse("2024-02-22T13:50:05Z"),
                                    withJobSummaries(summary(jobResult,
                                            Instant.parse("2024-02-22T13:50:01Z"),
                                            Instant.parse("2024-02-22T13:50:02Z"))))
                            .build());
        }

        @Test
        void shouldSaveTaskWhenMultipleJobsSucceed() throws Exception {
            // Given
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job 1 start
                    Instant.parse("2024-02-22T13:50:02Z"), // Job 1 finish
                    Instant.parse("2024-02-22T13:50:03Z"), // Job 2 start
                    Instant.parse("2024-02-22T13:50:04Z"), // Job 2 finish
                    Instant.parse("2024-02-22T13:50:05Z"))); // Finish
            createJobOnQueue("job1");
            createJobOnQueue("job2");

            // When
            IngestResult job1Result = recordsReadAndWritten(10L, 10L);
            IngestResult job2Result = recordsReadAndWritten(5L, 5L);
            runTask("test-task-1", processJobs(
                    jobSucceeds(job1Result),
                    jobSucceeds(job2Result)),
                    times::poll);

            // Then
            assertThat(taskStore.getAllTasks())
                    .containsExactly(IngestTaskStatus.builder()
                            .startTime(Instant.parse("2024-02-22T13:50:00Z"))
                            .taskId("test-task-1")
                            .finished(Instant.parse("2024-02-22T13:50:05Z"),
                                    withJobSummaries(
                                            summary(job1Result,
                                                    Instant.parse("2024-02-22T13:50:01Z"),
                                                    Instant.parse("2024-02-22T13:50:02Z")),
                                            summary(job2Result,
                                                    Instant.parse("2024-02-22T13:50:03Z"),
                                                    Instant.parse("2024-02-22T13:50:04Z"))))
                            .build());
        }

        @Test
        void shouldSaveTaskWhenOneJobFails() throws Exception {
            // Given
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job starts
                    Instant.parse("2024-02-22T13:50:05Z"))); // Finish
            createJobOnQueue("job1");

            // When
            runTask("test-task-1", processJobs(jobFails()), times::poll);

            // Then
            assertThat(taskStore.getAllTasks())
                    .containsExactly(IngestTaskStatus.builder()
                            .startTime(Instant.parse("2024-02-22T13:50:00Z"))
                            .taskId("test-task-1")
                            .finished(Instant.parse("2024-02-22T13:50:05Z"), noJobSummaries())
                            .build());
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
            assertThat(taskStore.getAllTasks())
                    .containsExactly(IngestTaskStatus.builder()
                            .startTime(Instant.parse("2024-02-22T13:50:00Z"))
                            .taskId("test-task-1")
                            .finished(Instant.parse("2024-02-22T13:50:05Z"), noJobSummaries())
                            .build());
        }

        private IngestTaskFinishedStatus.Builder noJobSummaries() {
            return withJobSummaries();
        }

        private IngestTaskFinishedStatus.Builder withJobSummaries(RecordsProcessedSummary... summaries) {
            IngestTaskFinishedStatus.Builder taskFinishedBuilder = IngestTaskFinishedStatus.builder();
            Stream.of(summaries).forEach(taskFinishedBuilder::addJobSummary);
            return taskFinishedBuilder;
        }
    }

    private void runTask(IngestJobHandler ingestRunner) throws Exception {
        runTask(ingestRunner, Instant::now);
    }

    private void runTask(IngestJobHandler ingestRunner, Supplier<Instant> timeSupplier) throws Exception {
        runTask(pollQueue(), ingestRunner, timeSupplier, DEFAULT_TASK_ID);
    }

    private void runTask(String taskId, IngestJobHandler ingestRunner, Supplier<Instant> timeSupplier) throws Exception {
        runTask(pollQueue(), ingestRunner, timeSupplier, taskId);
    }

    private void runTask(
            MessageReceiver messageReceiver,
            IngestJobHandler ingestRunner,
            Supplier<Instant> timeSupplier,
            String taskId) throws Exception {
        new NewIngestTask(timeSupplier, messageReceiver, ingestRunner, jobStore, taskStore, taskId)
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
                .tableId("test-table-id")
                .tableName("test-table")
                .files(UUID.randomUUID().toString())
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
        return new ProcessJob(true, result);
    }

    private ProcessJob jobSucceeds() {
        return new ProcessJob(true, defaultFileIngestResult("test-file"));
    }

    private ProcessJob jobFails() {
        return new ProcessJob(false, defaultFileIngestResult("test-file"));
    }

    private IngestJobHandler processNoJobs() {
        return processJobs();
    }

    private IngestJobHandler processJobs(ProcessJob... actions) {
        Iterator<ProcessJob> getAction = List.of(actions).iterator();
        return job -> {
            if (getAction.hasNext()) {
                ProcessJob action = getAction.next();
                try {
                    action.run(job);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return action.result;
            } else {
                throw new IllegalStateException("Unexpected job: " + job);
            }
        };
    }

    private class ProcessJob {
        private final boolean succeed;
        private final IngestResult result;

        ProcessJob(boolean succeed, IngestResult result) {
            this.succeed = succeed;
            this.result = result;
        }

        public void run(IngestJob job) throws Exception {
            if (succeed) {
                successfulJobs.add(job);
            } else {
                throw new Exception("Failed to process job");
            }
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
