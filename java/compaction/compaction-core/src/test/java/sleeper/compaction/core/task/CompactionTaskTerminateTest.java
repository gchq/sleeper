/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.compaction.core.task;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_ALIVE_TIME_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS;

public class CompactionTaskTerminateTest extends CompactionTaskTestBase {

    @Nested
    @DisplayName("Stop if idle for a specified period")
    class StopAfterMaxIdleTime {

        @Test
        void shouldTerminateIfNoJobsArePresentAfterRunningForIdleTime() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 3);
            instanceProperties.setNumber(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS, 2);
            Supplier<Instant> supplier = timeSupplier(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Keep alive check
                    Instant.parse("2024-02-22T13:50:03Z")); // First check

            // When
            runTask(processNoJobs(), supplier);

            // Then
            assertThat(assertSupplierEmpty(supplier)).isTrue();
            assertThat(sleeps).isEmpty();
        }

        @Test
        void shouldTerminateIfNoJobsArePresentAfterRunningForIdleTimeWithTwoQueuePolls() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 3);
            instanceProperties.setNumber(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS, 2);
            Supplier<Instant> supplier = timeSupplier(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Keep alive check
                    Instant.parse("2024-02-22T13:50:02Z"), // First idle time check
                    Instant.parse("2024-02-22T13:50:03Z"), // Keep alive check
                    Instant.parse("2024-02-22T13:50:04Z")); // Second idle time check + finish

            // When
            runTask(processNoJobs(), supplier);

            // Then
            assertThat(assertSupplierEmpty(supplier)).isTrue();
            assertThat(sleeps).containsExactly(Duration.ofSeconds(2));
        }

        @Test
        void shouldTerminateAfterRunningJobAndWaitingForIdleTime() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 3);
            instanceProperties.setNumber(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS, 2);
            Supplier<Instant> supplier = timeSupplier(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Keep alive check
                    Instant.parse("2024-02-22T13:50:01Z"), // Job started
                    Instant.parse("2024-02-22T13:50:02Z"), // Job completed
                    Instant.parse("2024-02-22T13:50:02Z"), // Job committed
                    Instant.parse("2024-02-22T13:50:03Z"), // Keep alive check
                    Instant.parse("2024-02-22T13:50:05Z")); // Idle time check with empty queue and finish

            CompactionJob job = createJobOnQueue("job1");

            // When
            runTask(jobsSucceed(1), supplier);

            // Then
            assertThat(assertSupplierEmpty(supplier)).isTrue();
            assertThat(consumedJobs).containsExactly(job);
            assertThat(jobsReturnedToQueue).isEmpty();
            assertThat(jobsOnQueue).isEmpty();
            assertThat(sleeps).isEmpty();
        }

        @Test
        void shouldTerminateWhenMaxIdleTimeNotMetOnFirstCheckThenIdleAfterProcessingJob() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 3);
            instanceProperties.setNumber(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS, 2);
            Supplier<Instant> supplier = timeSupplier(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Keep alive check
                    Instant.parse("2024-02-22T13:50:01Z"), // First check no job
                    Instant.parse("2024-02-22T13:50:02Z"), // Keep alive check
                    Instant.parse("2024-02-22T13:50:02Z"), // Second check Job started
                    Instant.parse("2024-02-22T13:50:02Z"), // Job completed
                    Instant.parse("2024-02-22T13:50:02Z"), // Job committed
                    Instant.parse("2024-02-22T13:50:04Z"), // Keep alive check
                    Instant.parse("2024-02-22T13:50:06Z")); // Third check + finish
            CompactionJob job = createJob("job1");

            // When
            runTask(
                    pollQueue(
                            receiveNoJobAnd(() -> send(job)),
                            receiveJob(),
                            receiveNoJob()),
                    processJobs(jobSucceeds()),
                    supplier);

            // Then
            assertThat(assertSupplierEmpty(supplier)).isTrue();
            assertThat(consumedJobs).containsExactly(job);
            assertThat(jobsReturnedToQueue).isEmpty();
            assertThat(jobsOnQueue).isEmpty();
            assertThat(sleeps).containsExactly(Duration.ofSeconds(2));
        }

        @Test
        void shouldTerminateWhenMaxIdleTimeNotMetOnFirstCheckThenNotMetAfterProcessingJob() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 3);
            instanceProperties.setNumber(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS, 2);
            Supplier<Instant> supplier = timeSupplier(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Keep alive check
                    Instant.parse("2024-02-22T13:50:01Z"), // First check no job
                    Instant.parse("2024-02-22T13:50:02Z"), // Keep alive check
                    Instant.parse("2024-02-22T13:50:02Z"), // Second check Job started
                    Instant.parse("2024-02-22T13:50:03Z"), // Job completed
                    Instant.parse("2024-02-22T13:50:03Z"), // Job committed
                    Instant.parse("2024-02-22T13:50:04Z"), // Keep alive check
                    Instant.parse("2024-02-22T13:50:04Z"), // Third check
                    Instant.parse("2024-02-22T13:50:06Z"), // Keep alive check
                    Instant.parse("2024-02-22T13:50:06Z")); // Fourth check + finish
            CompactionJob job = createJob("job1");

            // When
            runTask(
                    pollQueue(
                            receiveNoJobAnd(() -> send(job)),
                            receiveJob(),
                            receiveNoJob(),
                            receiveNoJob()),
                    processJobs(jobSucceeds()),
                    supplier);

            // Then
            assertThat(assertSupplierEmpty(supplier)).isTrue();
            assertThat(consumedJobs).containsExactly(job);
            assertThat(jobsReturnedToQueue).isEmpty();
            assertThat(jobsOnQueue).isEmpty();
            assertThat(sleeps).containsExactly(Duration.ofSeconds(2), Duration.ofSeconds(2));
        }

        @Test
        void shouldNotDelayRetryIfSetToZero() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 3);
            instanceProperties.setNumber(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS, 0);
            Supplier<Instant> supplier = timeSupplier(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Keep alive check
                    Instant.parse("2024-02-22T13:50:02Z"), // First idle time check
                    Instant.parse("2024-02-22T13:50:03Z"), // Keep alive check
                    Instant.parse("2024-02-22T13:50:04Z")); // Second idle time check + finish

            // When
            runTask(processNoJobs(), supplier);

            // Then
            assertThat(assertSupplierEmpty(supplier)).isTrue();
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
            assertThat(consumedJobs).isEmpty();
            assertThat(jobsReturnedToQueue).containsExactly(job1, job2);
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
            assertThat(consumedJobs).containsExactly(job2, job4);
            assertThat(jobsReturnedToQueue).containsExactly(job1, job3);
            assertThat(jobsOnQueue).isEmpty();
        }
    }

    @Nested
    @DisplayName("Stop after max alive time")
    class StopAfterMaxAliveTime {

        @Test
        void shouldStopTaskAfterMaxAliveTime() throws Exception {
            //Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_ALIVE_TIME_IN_SECONDS, 2);
            Supplier<Instant> supplier = timeSupplier(
                    Instant.parse("2024-02-22T13:50:00Z"), // Start
                    Instant.parse("2024-02-22T13:50:01Z"), // Keep alive check
                    Instant.parse("2024-02-22T13:50:02Z"), // Job1 started
                    Instant.parse("2024-02-22T13:50:03Z"), // Job1 completed
                    Instant.parse("2024-02-22T13:50:03Z"), // Job1 committed
                    Instant.parse("2024-02-22T13:50:03Z"), // Keep alive check
                    Instant.parse("2024-02-22T13:50:04Z")); // Finish
            CompactionJob job1 = createJobOnQueue("job1");
            CompactionJob job2 = createJobOnQueue("job2");

            // When
            runTask(jobsSucceed(2), supplier);

            // Then
            assertThat(assertSupplierEmpty(supplier)).isTrue();
            assertThat(sleeps).isEmpty();
            assertThat(consumedJobs).containsExactly(job1);
            assertThat(jobsOnQueue).containsExactly(job2);
        }
    }
}
