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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.execution.CompactionTask.JobAndMessage;
import sleeper.compaction.job.execution.CompactionTask.MessageConsumer;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS;

public class CompactionTaskTest {

    private final InstanceProperties instanceProperties = createInstance();
    private final Queue<CompactionJob> jobsOnQueue = new LinkedList<>();
    private final List<CompactionJob> successfulJobs = new ArrayList<>();
    private final List<CompactionJob> failedJobs = new ArrayList<>();

    @Nested
    @DisplayName("Process jobs")
    class ProcessJobs {

        @BeforeEach
        void setUp() {
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 0);
        }

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
            runTask(withFailingJobs(job));

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
            runTask(withFailingJobs(job2));

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
            Iterator<Instant> times = List.of(
                    Instant.parse("2024-02-22T13:50:00Z"),
                    Instant.parse("2024-02-22T13:50:03Z")).iterator();

            // When
            runTaskWithTimes(processNoJobs(), times::next);

            // Then
            assertThat(times).isExhausted();
        }

        @Test
        void shouldTerminateIfNoJobsArePresentAfterRunningForIdleTimeWithTwoQueuePolls() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 3);
            Iterator<Instant> times = List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:02Z"), // First idle time check
                    Instant.parse("2024-02-22T13:50:04Z")) // Second idle time check
                    .iterator();

            // When
            runTaskWithTimes(processNoJobs(), times::next);

            // Then
            assertThat(times).isExhausted();
        }

        @Test
        void shouldTerminateAfterRunningJobAndWaitingForIdleTime() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 3);
            Iterator<Instant> times = List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Job completed
                    Instant.parse("2024-02-22T13:50:05Z")) // Idle time check with empty queue
                    .iterator();
            CompactionJob job = createJobOnQueue("job1");

            // When
            runTaskWithTimes(jobsSucceed(1), times::next);

            // Then
            assertThat(times).isExhausted();
            assertThat(successfulJobs).containsExactly(job);
            assertThat(failedJobs).isEmpty();
            assertThat(jobsOnQueue).isEmpty();
        }

        @Test
        @Disabled("TODO")
        void shouldTerminateWhenQueueIsEmptyOnFirstCheckThenIdleAfterProcessingJob() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 3);
            Iterator<Instant> times = List.of(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:02Z"), // First check
                    Instant.parse("2024-02-22T13:50:04Z"), // Job completed
                    Instant.parse("2024-02-22T13:50:06Z")) // Second check
                    .iterator();
            CompactionJob job = createJobOnQueue("job1");

            // When
            runTaskWithTimes(jobsSucceed(1), times::next);

            // Then
            assertThat(times).isExhausted();
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

    private static InstanceProperties createInstance() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 0);
        instanceProperties.setNumber(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES, 2);
        return instanceProperties;
    }

    private void runTask(MessageConsumer messageConsumer) throws Exception {
        runTaskWithTimes(messageConsumer, Instant::now);
    }

    private void runTaskWithTimes(MessageConsumer messageConsumer, Supplier<Instant> timeSupplier) throws Exception {
        new CompactionTask(instanceProperties, timeSupplier, () -> {
            CompactionJob job = jobsOnQueue.poll();
            if (job != null) {
                return Optional.of(new JobAndMessage(job, null));
            } else {
                return Optional.empty();
            }
        }, messageConsumer, (jobAndMessage) -> failedJobs.add(jobAndMessage.getJob()))
                .runAt(timeSupplier.get());
    }

    private CompactionJob createJobOnQueue(String jobId) {
        CompactionJob job = CompactionJob.builder()
                .tableId("test-table-id")
                .jobId(jobId)
                .partitionId("root")
                .inputFiles(List.of(UUID.randomUUID().toString()))
                .outputFile(UUID.randomUUID().toString()).build();
        jobsOnQueue.add(job);
        return job;
    }

    private MessageConsumer jobsSucceed(int numJobs) {
        return processJobs(Stream.generate(() -> jobSucceeds())
                .limit(numJobs)
                .toArray(MessageConsumer[]::new));
    }

    private MessageConsumer jobSucceeds() {
        return (jobAndMessage) -> successfulJobs.add(jobAndMessage.getJob());
    }

    private MessageConsumer jobFails() {
        return jobAndMessage -> {
            throw new Exception("Failed to process job");
        };
    }

    private MessageConsumer processNoJobs() {
        return processJobs();
    }

    private MessageConsumer processJobs(MessageConsumer... actions) {
        Iterator<MessageConsumer> getAction = List.of(actions).iterator();
        return (jobAndMessage) -> {
            if (getAction.hasNext()) {
                getAction.next().consume(jobAndMessage);
            } else {
                throw new IllegalStateException("Unexpected job: " + jobAndMessage);
            }
        };
    }

    private MessageConsumer withFailingJobs(CompactionJob... jobs) {
        Set<String> failingJobIds = Stream.of(jobs).map(CompactionJob::getId).collect(Collectors.toSet());
        return (jobAndMessage) -> {
            if (failingJobIds.contains(jobAndMessage.getJob().getId())) {
                throw new Exception("Failed to process job");
            } else {
                successfulJobs.add(jobAndMessage.getJob());
            }
        };
    }
}
