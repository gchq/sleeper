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

import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.task.NewIngestTask.IngestRunner;
import sleeper.ingest.task.NewIngestTask.MessageHandle;
import sleeper.ingest.task.NewIngestTask.MessageReceiver;

import java.time.Duration;
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
import static sleeper.core.record.process.RecordsProcessedSummaryTestData.summary;

public class NewIngestTaskTest {
    private static final String DEFAULT_TASK_ID = "test-task-id";
    private static final Instant DEFAULT_START_TIME = Instant.parse("2024-03-04T11:00:00Z");
    private static final Duration DEFAULT_DURATION = Duration.ofSeconds(5);

    private final Queue<IngestJob> jobsOnQueue = new LinkedList<>();
    private final List<IngestJob> successfulJobs = new ArrayList<>();
    private final List<IngestJob> failedJobs = new ArrayList<>();
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
                    Instant.parse("2024-02-22T13:50:05Z"))); // Finish
            createJobOnQueue("job1");

            // When
            RecordsProcessedSummary jobSummary = summary(
                    Instant.parse("2024-02-22T13:50:01Z"),
                    Instant.parse("2024-02-22T13:50:02Z"), 10L, 10L);
            runTask("test-task-1", processJobs(
                    jobSucceeds(jobSummary)),
                    times::poll);

            // Then
            assertThat(taskStore.getAllTasks())
                    .containsExactly(IngestTaskStatus.builder()
                            .startTime(Instant.parse("2024-02-22T13:50:00Z"))
                            .taskId("test-task-1")
                            .finished(Instant.parse("2024-02-22T13:50:05Z"),
                                    withJobSummaries(jobSummary))
                            .build());
        }

        @Test
        void shouldSaveTaskWhenMultipleJobsSucceed() throws Exception {
            // Given
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:05Z"))); // Finish
            createJobOnQueue("job1");
            createJobOnQueue("job2");

            // When
            RecordsProcessedSummary job1Summary = summary(
                    Instant.parse("2024-02-22T13:50:01Z"),
                    Instant.parse("2024-02-22T13:50:02Z"), 10L, 10L);
            RecordsProcessedSummary job2Summary = summary(
                    Instant.parse("2024-02-22T13:50:03Z"),
                    Instant.parse("2024-02-22T13:50:04Z"), 5L, 5L);
            runTask("test-task-1", processJobs(
                    jobSucceeds(job1Summary),
                    jobSucceeds(job2Summary)),
                    times::poll);

            // Then
            assertThat(taskStore.getAllTasks())
                    .containsExactly(IngestTaskStatus.builder()
                            .startTime(Instant.parse("2024-02-22T13:50:00Z"))
                            .taskId("test-task-1")
                            .finished(Instant.parse("2024-02-22T13:50:05Z"),
                                    withJobSummaries(job1Summary, job2Summary))
                            .build());
        }

        @Test
        void shouldSaveTaskWhenOneJobFails() throws Exception {
            // Given
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
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

    private void runTask(IngestRunner ingestRunner) throws Exception {
        runTask(ingestRunner, Instant::now);
    }

    private void runTask(IngestRunner ingestRunner, Supplier<Instant> timeSupplier) throws Exception {
        runTask(pollQueue(), ingestRunner, timeSupplier, DEFAULT_TASK_ID);
    }

    private void runTask(String taskId, IngestRunner ingestRunner, Supplier<Instant> timeSupplier) throws Exception {
        runTask(pollQueue(), ingestRunner, timeSupplier, taskId);
    }

    private void runTask(
            MessageReceiver messageReceiver,
            IngestRunner ingestRunner,
            Supplier<Instant> timeSupplier) throws Exception {
        runTask(messageReceiver, ingestRunner, timeSupplier, DEFAULT_TASK_ID);
    }

    private void runTask(
            MessageReceiver messageReceiver,
            IngestRunner ingestRunner,
            Supplier<Instant> timeSupplier,
            String taskId) throws Exception {
        new NewIngestTask(timeSupplier, messageReceiver, ingestRunner, taskStore, taskId)
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

    private IngestRunner jobsSucceed(int numJobs) {
        return processJobs(Stream.generate(() -> jobSucceeds())
                .limit(numJobs)
                .toArray(ProcessJob[]::new));
    }

    private ProcessJob jobSucceeds(RecordsProcessedSummary summary) {
        return new ProcessJob(true, summary);
    }

    private ProcessJob jobSucceeds() {
        return new ProcessJob(true, 10L, DEFAULT_START_TIME, DEFAULT_DURATION);
    }

    private ProcessJob jobFails() {
        return new ProcessJob(false, 0L, DEFAULT_START_TIME, DEFAULT_DURATION);
    }

    private IngestRunner processNoJobs() {
        return processJobs();
    }

    private IngestRunner processJobs(ProcessJob... actions) {
        Iterator<ProcessJob> getAction = List.of(actions).iterator();
        return job -> {
            if (getAction.hasNext()) {
                ProcessJob action = getAction.next();
                try {
                    action.run(job);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return action.summary;
            } else {
                throw new IllegalStateException("Unexpected job: " + job);
            }
        };
    }

    private class ProcessJob {
        private final boolean succeed;
        private final RecordsProcessedSummary summary;

        ProcessJob(boolean succeed, long records, Instant startTime, Duration duration) {
            this(succeed, new RecordsProcessedSummary(new RecordsProcessed(records, records), startTime, duration));
        }

        ProcessJob(boolean succeed, RecordsProcessedSummary summary) {
            this.succeed = succeed;
            this.summary = summary;
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
