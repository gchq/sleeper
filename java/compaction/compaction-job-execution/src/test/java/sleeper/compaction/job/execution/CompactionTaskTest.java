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
import sleeper.compaction.job.execution.CompactionTask.CompactionRunner;
import sleeper.compaction.job.execution.CompactionTask.MessageHandle;
import sleeper.compaction.job.execution.CompactionTask.MessageReceiver;
import sleeper.compaction.task.CompactionTaskFinishedStatus;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.compaction.testutils.InMemoryCompactionTaskStatusStore;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;

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
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS;
import static sleeper.core.record.process.RecordsProcessedSummaryTestData.summary;

public class CompactionTaskTest {
    private static final String DEFAULT_TASK_ID = "test-task-id";
    private static final Instant DEFAULT_START_TIME = Instant.parse("2024-03-04T11:00:00Z");
    private static final Duration DEFAULT_DURATION = Duration.ofSeconds(5);

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Queue<CompactionJob> jobsOnQueue = new LinkedList<>();
    private final List<CompactionJob> successfulJobs = new ArrayList<>();
    private final List<CompactionJob> failedJobs = new ArrayList<>();
    private final CompactionTaskStatusStore taskStore = new InMemoryCompactionTaskStatusStore();

    @BeforeEach
    void setUp() {
        instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 0);
        instanceProperties.setNumber(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES, 10);
    }

    @Nested
    @DisplayName("Process jobs")
    class ProcessJobs {

        @Test
        void shouldRunJobFromQueueThenTerminate() throws Exception {
            // Given
            CompactionJob job = createJobOnQueue("job1");

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
            CompactionJob job = createJobOnQueue("job1");

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
            CompactionJob job1 = createJobOnQueue("job1");
            CompactionJob job2 = createJobOnQueue("job2");

            // When
            runTask(processJobs(jobSucceeds(), jobFails()));

            // Then
            assertThat(successfulJobs).containsExactly(job1);
            assertThat(failedJobs).containsExactly(job2);
            assertThat(jobsOnQueue).isEmpty();
        }
    }

    @Nested
    @DisplayName("Stop if idle for a specified period")
    class StopAfterMaxIdleTime {

        @Test
        void shouldTerminateIfNoJobsArePresentAfterRunningForIdleTime() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 3);
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:03Z"))); // Finish

            // When
            runTask(processNoJobs(), times::poll);

            // Then
            assertThat(times).isEmpty();
        }

        @Test
        void shouldTerminateIfNoJobsArePresentAfterRunningForIdleTimeWithTwoQueuePolls() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 3);
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:02Z"), // First idle time check
                    Instant.parse("2024-02-22T13:50:04Z"))); // Second idle time check + finish

            // When
            runTask(processNoJobs(), times::poll);

            // Then
            assertThat(times).isEmpty();
        }

        @Test
        void shouldTerminateAfterRunningJobAndWaitingForIdleTime() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 3);
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job completed
                    Instant.parse("2024-02-22T13:50:05Z"))); // Idle time check with empty queue and finish
            CompactionJob job = createJobOnQueue("job1");

            // When
            runTask(jobsSucceed(1), times::poll);

            // Then
            assertThat(times).isEmpty();
            assertThat(successfulJobs).containsExactly(job);
            assertThat(failedJobs).isEmpty();
            assertThat(jobsOnQueue).isEmpty();
        }

        @Test
        void shouldTerminateWhenMaxIdleTimeNotMetOnFirstCheckThenIdleAfterProcessingJob() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 3);
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // First check
                    Instant.parse("2024-02-22T13:50:02Z"), // Job completed
                    Instant.parse("2024-02-22T13:50:06Z"))); // Second check + finish
            CompactionJob job = createJob("job1");

            // When
            runTask(
                    pollQueue(
                            receiveNoJobAnd(() -> send(job)),
                            receiveJob(),
                            receiveNoJob()),
                    processJobs(jobSucceeds()),
                    times::poll);

            // Then
            assertThat(times).isEmpty();
            assertThat(successfulJobs).containsExactly(job);
            assertThat(failedJobs).isEmpty();
            assertThat(jobsOnQueue).isEmpty();
        }

        @Test
        void shouldTerminateWhenMaxIdleTimeNotMetOnFirstCheckThenNotMetAfterProcessingJob() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 3);
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // First check
                    Instant.parse("2024-02-22T13:50:02Z"), // Job completed
                    Instant.parse("2024-02-22T13:50:04Z"), // Second check
                    Instant.parse("2024-02-22T13:50:06Z"))); // Third check + finish
            CompactionJob job = createJob("job1");

            // When
            runTask(
                    pollQueue(
                            receiveNoJobAnd(() -> send(job)),
                            receiveJob(),
                            receiveNoJob(),
                            receiveNoJob()),
                    processJobs(jobSucceeds()),
                    times::poll);

            // Then
            assertThat(times).isEmpty();
            assertThat(successfulJobs).containsExactly(job);
            assertThat(failedJobs).isEmpty();
            assertThat(jobsOnQueue).isEmpty();
        }
    }

    @Nested
    @DisplayName("Stop if failed too many times consecutively")
    class StopAfterConsecutiveFailures {
        @Test
        void shouldStopEarlyIfMaxConsecutiveFailuresMet() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES, 2);
            CompactionJob job1 = createJobOnQueue("job1");
            CompactionJob job2 = createJobOnQueue("job2");
            CompactionJob job3 = createJobOnQueue("job3");

            // When
            runTask(processJobs(jobFails(), jobFails(), jobSucceeds()));

            // Then
            assertThat(successfulJobs).isEmpty();
            assertThat(failedJobs).containsExactly(job1, job2);
            assertThat(jobsOnQueue).containsExactly(job3);
        }

        @Test
        void shouldResetConsecutiveFailureCountIfJobProcessedSuccessfully() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES, 2);
            CompactionJob job1 = createJobOnQueue("job1");
            CompactionJob job2 = createJobOnQueue("job2");
            CompactionJob job3 = createJobOnQueue("job3");
            CompactionJob job4 = createJobOnQueue("job4");

            // When
            runTask(processJobs(jobFails(), jobSucceeds(), jobFails(), jobSucceeds()));

            // Then
            assertThat(successfulJobs).containsExactly(job2, job4);
            assertThat(failedJobs).containsExactly(job1, job3);
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
                    Instant.parse("2024-02-22T13:50:02Z"), // Job completed
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
                    .containsExactly(CompactionTaskStatus.builder()
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
                    Instant.parse("2024-02-22T13:50:02Z"), // Job 1 completed
                    Instant.parse("2024-02-22T13:50:04Z"), // Job 2 completed
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
                    .containsExactly(CompactionTaskStatus.builder()
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
                    .containsExactly(CompactionTaskStatus.builder()
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
                    .containsExactly(CompactionTaskStatus.builder()
                            .startTime(Instant.parse("2024-02-22T13:50:00Z"))
                            .taskId("test-task-1")
                            .finished(Instant.parse("2024-02-22T13:50:05Z"), noJobSummaries())
                            .build());
        }

        private CompactionTaskFinishedStatus.Builder noJobSummaries() {
            return withJobSummaries();
        }

        private CompactionTaskFinishedStatus.Builder withJobSummaries(RecordsProcessedSummary... summaries) {
            CompactionTaskFinishedStatus.Builder taskFinishedBuilder = CompactionTaskFinishedStatus.builder();
            Stream.of(summaries).forEach(taskFinishedBuilder::addJobSummary);
            return taskFinishedBuilder;
        }
    }

    private void runTask(CompactionRunner compactor) throws Exception {
        runTask(compactor, Instant::now);
    }

    private void runTask(CompactionRunner compactor, Supplier<Instant> timeSupplier) throws Exception {
        runTask(pollQueue(), compactor, timeSupplier, DEFAULT_TASK_ID);
    }

    private void runTask(String taskId, CompactionRunner compactor, Supplier<Instant> timeSupplier) throws Exception {
        runTask(pollQueue(), compactor, timeSupplier, taskId);
    }

    private void runTask(
            MessageReceiver messageReceiver,
            CompactionRunner compactor,
            Supplier<Instant> timeSupplier) throws Exception {
        runTask(messageReceiver, compactor, timeSupplier, DEFAULT_TASK_ID);
    }

    private void runTask(
            MessageReceiver messageReceiver,
            CompactionRunner compactor,
            Supplier<Instant> timeSupplier,
            String taskId) throws Exception {
        new CompactionTask(instanceProperties, timeSupplier, messageReceiver, compactor, taskStore, taskId)
                .run();
    }

    private CompactionJob createJobOnQueue(String jobId) {
        CompactionJob job = createJob(jobId);
        jobsOnQueue.add(job);
        return job;
    }

    private CompactionJob createJob(String jobId) {
        return CompactionJob.builder()
                .tableId("test-table-id")
                .jobId(jobId)
                .partitionId("root")
                .inputFiles(List.of(UUID.randomUUID().toString()))
                .outputFile(UUID.randomUUID().toString()).build();
    }

    private void send(CompactionJob job) {
        jobsOnQueue.add(job);
    }

    private MessageReceiver pollQueue() {
        return () -> {
            CompactionJob job = jobsOnQueue.poll();
            if (job != null) {
                return Optional.of(new FakeMessageHandle(job));
            } else {
                return Optional.empty();
            }
        };
    }

    private MessageReceiver pollQueue(MessageReceiver... actions) {
        Iterator<MessageReceiver> getAction = List.of(actions).iterator();
        return () -> {
            if (getAction.hasNext()) {
                return getAction.next().receiveMessage();
            } else {
                throw new IllegalStateException("Unexpected queue poll");
            }
        };
    }

    private MessageReceiver receiveJob() {
        return () -> {
            if (jobsOnQueue.isEmpty()) {
                throw new IllegalStateException("Expected job on queue");
            }
            return Optional.of(new FakeMessageHandle(jobsOnQueue.poll()));
        };
    }

    private MessageReceiver receiveNoJob() {
        return () -> {
            if (!jobsOnQueue.isEmpty()) {
                throw new IllegalStateException("Expected no jobs on queue");
            }
            return Optional.empty();
        };
    }

    private MessageReceiver receiveNoJobAnd(Runnable action) {
        return () -> {
            if (!jobsOnQueue.isEmpty()) {
                throw new IllegalStateException("Expected no jobs on queue");
            }
            action.run();
            return Optional.empty();
        };
    }

    private CompactionRunner jobsSucceed(int numJobs) {
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

    private CompactionRunner processNoJobs() {
        return processJobs();
    }

    private CompactionRunner processJobs(ProcessJob... actions) {
        Iterator<ProcessJob> getAction = List.of(actions).iterator();
        return job -> {
            if (getAction.hasNext()) {
                ProcessJob action = getAction.next();
                action.run(job);
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

        public void run(CompactionJob job) throws Exception {
            if (succeed) {
                successfulJobs.add(job);
            } else {
                throw new Exception("Failed to process job");
            }
        }
    }

    private class FakeMessageHandle implements MessageHandle {
        private final CompactionJob job;

        FakeMessageHandle(CompactionJob job) {
            this.job = job;
        }

        public CompactionJob getJob() {
            return job;
        }

        public void close() {
        }

        public void completed() {
        }

        public void failed() {
            failedJobs.add(job);
        }
    }
}
