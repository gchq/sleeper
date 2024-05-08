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
import sleeper.compaction.job.commit.CompactionJobCommitter;
import sleeper.compaction.job.execution.CompactionTask.CompactionRunner;
import sleeper.compaction.job.execution.CompactionTask.MessageHandle;
import sleeper.compaction.job.execution.CompactionTask.MessageReceiver;
import sleeper.compaction.task.CompactionTaskFinishedStatus;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.compaction.testutils.InMemoryCompactionJobStatusStore;
import sleeper.compaction.testutils.InMemoryCompactionTaskStatusStore;
import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;

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
import static sleeper.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.CompactionJobStatusTestData.startedCompactionRun;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_JOB_COMPLETION_ASYNC;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithSinglePartition;

public class CompactionTaskTest {
    private static final String DEFAULT_TABLE_ID = "test-table-id";
    private static final String DEFAULT_TASK_ID = "test-task-id";
    private static final Instant DEFAULT_CREATED_TIME = Instant.parse("2024-03-04T10:50:00Z");

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key");
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final StateStore stateStore = inMemoryStateStoreWithSinglePartition(schema);
    private final FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
    private final Queue<CompactionJob> jobsOnQueue = new LinkedList<>();
    private final List<CompactionJob> successfulJobs = new ArrayList<>();
    private final List<CompactionJob> failedJobs = new ArrayList<>();
    private final InMemoryCompactionJobStatusStore jobStore = new InMemoryCompactionJobStatusStore();
    private final CompactionTaskStatusStore taskStore = new InMemoryCompactionTaskStatusStore();
    private final List<Duration> sleeps = new ArrayList<>();
    private final List<CompactionJobCommitRequest> commitRequestsOnQueue = new ArrayList<>();

    @BeforeEach
    void setUp() {
        instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 0);
        instanceProperties.setNumber(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES, 10);
        tableProperties.set(TABLE_ID, DEFAULT_TABLE_ID);
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
            instanceProperties.setNumber(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS, 2);
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:03Z"))); // Finish

            // When
            runTask(processNoJobs(), times::poll);

            // Then
            assertThat(times).isEmpty();
            assertThat(sleeps).isEmpty();
        }

        @Test
        void shouldTerminateIfNoJobsArePresentAfterRunningForIdleTimeWithTwoQueuePolls() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 3);
            instanceProperties.setNumber(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS, 2);
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:02Z"), // First idle time check
                    Instant.parse("2024-02-22T13:50:04Z"))); // Second idle time check + finish

            // When
            runTask(processNoJobs(), times::poll);

            // Then
            assertThat(times).isEmpty();
            assertThat(sleeps).containsExactly(Duration.ofSeconds(2));
        }

        @Test
        void shouldTerminateAfterRunningJobAndWaitingForIdleTime() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 3);
            instanceProperties.setNumber(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS, 2);
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job started
                    Instant.parse("2024-02-22T13:50:02Z"), // Job completed
                    Instant.parse("2024-02-22T13:50:05Z"))); // Idle time check with empty queue and finish
            CompactionJob job = createJobOnQueue("job1");

            // When
            runTask(jobsSucceed(1), times::poll);

            // Then
            assertThat(times).isEmpty();
            assertThat(successfulJobs).containsExactly(job);
            assertThat(failedJobs).isEmpty();
            assertThat(jobsOnQueue).isEmpty();
            assertThat(sleeps).isEmpty();
        }

        @Test
        void shouldTerminateWhenMaxIdleTimeNotMetOnFirstCheckThenIdleAfterProcessingJob() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 3);
            instanceProperties.setNumber(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS, 2);
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // First check
                    Instant.parse("2024-02-22T13:50:02Z"), // Job started
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
            assertThat(sleeps).containsExactly(Duration.ofSeconds(2));
        }

        @Test
        void shouldTerminateWhenMaxIdleTimeNotMetOnFirstCheckThenNotMetAfterProcessingJob() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 3);
            instanceProperties.setNumber(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS, 2);
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // First check
                    Instant.parse("2024-02-22T13:50:02Z"), // Job started
                    Instant.parse("2024-02-22T13:50:03Z"), // Job completed
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
            assertThat(sleeps).containsExactly(Duration.ofSeconds(2), Duration.ofSeconds(2));
        }

        @Test
        void shouldNotDelayRetryIfSetToZero() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 3);
            instanceProperties.setNumber(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS, 0);
            Queue<Instant> times = new LinkedList<>(List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:02Z"), // First idle time check
                    Instant.parse("2024-02-22T13:50:04Z"))); // Second idle time check + finish

            // When
            runTask(processNoJobs(), times::poll);

            // Then
            assertThat(times).isEmpty();
            assertThat(sleeps).isEmpty();
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
    @DisplayName("Update status stores")
    class UpdateStatusStores {
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
                    Instant.parse("2024-02-22T13:50:01Z"), // Job started
                    Instant.parse("2024-02-22T13:50:05Z"))); // Finish
            CompactionJob job = createJobOnQueue("job1");

            // When
            runTask("test-task-1", processJobs(jobFails()), times::poll);

            // Then
            assertThat(taskStore.getAllTasks()).containsExactly(
                    finishedCompactionTask("test-task-1",
                            Instant.parse("2024-02-22T13:50:00Z"),
                            Instant.parse("2024-02-22T13:50:05Z")));
            assertThat(jobStore.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                    jobCreated(job, DEFAULT_CREATED_TIME,
                            startedCompactionRun("test-task-1", Instant.parse("2024-02-22T13:50:01Z"))));
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

    @Nested
    @DisplayName("Send commits to queue")
    class SendCommitsToQueue {

        @BeforeEach
        public void setup() {
            tableProperties.set(COMPACTION_JOB_COMPLETION_ASYNC, "true");
        }

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
        }

        private CompactionJobCommitRequest commitRequestFor(CompactionJob job, RecordsProcessedSummary summary) {
            return new CompactionJobCommitRequest(job, DEFAULT_TASK_ID, summary);
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
        CompactionJobCommitHandler commitHandler = new CompactionJobCommitHandler(
                new FixedTablePropertiesProvider(tableProperties),
                new CompactionJobCommitter(jobStore, tableId -> stateStore),
                commitRequestsOnQueue::add);
        new CompactionTask(instanceProperties,
                PropertiesReloader.neverReload(), messageReceiver, compactor,
                commitHandler, jobStore, taskStore, taskId, timeSupplier, sleeps::add)
                .run();
    }

    private CompactionJob createJobOnQueue(String jobId) throws Exception {
        CompactionJob job = createJob(jobId);
        jobsOnQueue.add(job);
        jobStore.jobCreated(job, DEFAULT_CREATED_TIME);
        return job;
    }

    private CompactionJob createJob(String jobId) throws Exception {
        String inputFile = UUID.randomUUID().toString();
        CompactionJob job = CompactionJob.builder()
                .tableId(DEFAULT_TABLE_ID)
                .jobId(jobId)
                .partitionId("root")
                .inputFiles(List.of(inputFile))
                .outputFile(UUID.randomUUID().toString()).build();
        stateStore.addFile(factory.rootFile(inputFile, 123L));
        stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles(jobId, job.getPartitionId(), job.getInputFiles())));
        return job;
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

    private ProcessJob jobSucceeds(RecordsProcessed summary) {
        return new ProcessJob(true, summary);
    }

    private ProcessJob jobSucceeds() {
        return new ProcessJob(true, 10L);
    }

    private ProcessJob jobFails() {
        return new ProcessJob(false, 0L);
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
        private final RecordsProcessed summary;

        ProcessJob(boolean succeed, long records) {
            this(succeed, new RecordsProcessed(records, records));
        }

        ProcessJob(boolean succeed, RecordsProcessed summary) {
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
