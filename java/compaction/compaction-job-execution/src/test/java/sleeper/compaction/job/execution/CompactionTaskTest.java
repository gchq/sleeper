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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.task.CompactionTaskFinishedStatus;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.CompactionJobStatusTestData.startedCompactionRun;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS;

public class CompactionTaskTest extends CompactionTaskTestBase {

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
}
