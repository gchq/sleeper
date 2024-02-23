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
import sleeper.compaction.job.execution.CompactionTask.JobAndMessage;
import sleeper.compaction.job.execution.CompactionTask.MessageConsumer;
import sleeper.compaction.job.execution.CompactionTask.Result;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.time.Instant;
import java.util.ArrayList;
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
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_TIME_IN_SECONDS;

public class CompactionTaskTest {

    private final InstanceProperties instanceProperties = createInstance();
    private static final Queue<CompactionJob> JOBS_ON_QUEUE = new LinkedList<>();
    private static final List<CompactionJob> SUCCESSFUL_JOBS = new ArrayList<>();
    private static final List<CompactionJob> FAILED_JOBS = new ArrayList<>();

    @BeforeEach
    void setUp() {
        JOBS_ON_QUEUE.clear();
        SUCCESSFUL_JOBS.clear();
        FAILED_JOBS.clear();
    }

    @Nested
    @DisplayName("Process jobs")
    class ProcessJobs {
        @Test
        void shouldProcessSuccessfulJob() throws Exception {
            // Given
            CompactionJob job = createJobOnQueue("job1");

            // When
            Result result = runTask(allJobsSucceed());

            // Then
            assertThat(result.getTotalMessagesProcessed()).isEqualTo(1L);
            assertThat(result.hasMaxTimeExceeded()).isFalse();
            assertThat(result.hasMaxConsecutiveFailuresBeenReached()).isTrue();
            assertThat(SUCCESSFUL_JOBS).containsExactly(job);
            assertThat(FAILED_JOBS).isEmpty();
            assertThat(JOBS_ON_QUEUE).isEmpty();
        }

        @Test
        void shouldProcessFailingJob() throws Exception {
            // Given
            CompactionJob job = createJobOnQueue("job1");

            // When
            Result result = runTask(withFailingJobs(job));

            // Then
            assertThat(result.getTotalMessagesProcessed()).isEqualTo(0L);
            assertThat(result.hasMaxTimeExceeded()).isFalse();
            assertThat(result.hasMaxConsecutiveFailuresBeenReached()).isTrue();
            assertThat(SUCCESSFUL_JOBS).isEmpty();
            assertThat(FAILED_JOBS).containsExactly(job);
            assertThat(JOBS_ON_QUEUE).isEmpty();
        }

        @Test
        void shouldProcessSuccessfulThenFailingJob() throws Exception {
            // Given
            CompactionJob job1 = createJobOnQueue("job1");
            CompactionJob job2 = createJobOnQueue("job2");

            // When
            Result result = runTask(withFailingJobs(job2));

            // Then=
            assertThat(result.getTotalMessagesProcessed()).isEqualTo(1L);
            assertThat(result.hasMaxTimeExceeded()).isFalse();
            assertThat(result.hasMaxConsecutiveFailuresBeenReached()).isTrue();
            assertThat(SUCCESSFUL_JOBS).containsExactly(job1);
            assertThat(FAILED_JOBS).containsExactly(job2);
            assertThat(JOBS_ON_QUEUE).isEmpty();
        }
    }

    @Nested
    @DisplayName("Stop early if conditions are met")
    class StopEarly {
        @Test
        void shouldStopEarlyIfMaxConsecutiveFailuresMet() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES, 2);
            CompactionJob job1 = createJobOnQueue("job1");
            CompactionJob job2 = createJobOnQueue("job2");
            CompactionJob job3 = createJobOnQueue("job3");

            // When
            Result result = runTask(withFailingJobs(job1, job2));

            // Then
            assertThat(result.getTotalMessagesProcessed()).isEqualTo(0L);
            assertThat(result.hasMaxTimeExceeded()).isFalse();
            assertThat(result.hasMaxConsecutiveFailuresBeenReached()).isTrue();
            assertThat(SUCCESSFUL_JOBS).isEmpty();
            assertThat(FAILED_JOBS).containsExactly(job1, job2);
            assertThat(JOBS_ON_QUEUE).containsExactly(job3);
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
            Result result = runTask(withFailingJobs(job1, job3));

            // Then
            assertThat(result.getTotalMessagesProcessed()).isEqualTo(2L);
            assertThat(result.hasMaxTimeExceeded()).isFalse();
            assertThat(result.hasMaxConsecutiveFailuresBeenReached()).isTrue();
            assertThat(SUCCESSFUL_JOBS).containsExactly(job2, job4);
            assertThat(FAILED_JOBS).containsExactly(job1, job3);
            assertThat(JOBS_ON_QUEUE).isEmpty();
        }

        @Test
        void shouldStopEarlyIfMaxTimeWasReached() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_TIME_IN_SECONDS, 3);
            Supplier<Instant> timeSupplier = List.of(
                    Instant.parse("2024-02-22T13:50:00Z"),
                    Instant.parse("2024-02-22T13:50:05Z"),
                    Instant.parse("2024-02-22T13:50:10Z")).iterator()::next;
            CompactionJob job1 = createJobOnQueue("job1");
            CompactionJob job2 = createJobOnQueue("job2");

            // When
            Result result = runTaskWithTimes(allJobsSucceed(), timeSupplier);

            // Then
            assertThat(result.getTotalMessagesProcessed()).isEqualTo(1L);
            assertThat(result.hasMaxTimeExceeded()).isTrue();
            assertThat(result.hasMaxConsecutiveFailuresBeenReached()).isFalse();
            assertThat(SUCCESSFUL_JOBS).containsExactly(job1);
            assertThat(FAILED_JOBS).isEmpty();
            assertThat(JOBS_ON_QUEUE).containsExactly(job2);
        }
    }

    private static InstanceProperties createInstance() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.setNumber(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES, 2);
        return instanceProperties;
    }

    private Result runTask(MessageConsumer messageConsumer) throws Exception {
        return runTaskWithTimes(messageConsumer, Instant::now);
    }

    private Result runTaskWithTimes(MessageConsumer messageConsumer, Supplier<Instant> timeSupplier) throws Exception {
        return new CompactionTask(instanceProperties, timeSupplier, () -> {
            CompactionJob job = JOBS_ON_QUEUE.poll();
            if (job != null) {
                return Optional.of(new JobAndMessage(job, null));
            } else {
                return Optional.empty();
            }
        }, messageConsumer, (jobAndMessage) -> FAILED_JOBS.add(jobAndMessage.getJob())).runAt(timeSupplier.get());
    }

    private static CompactionJob createJobOnQueue(String jobId) {
        CompactionJob job = CompactionJob.builder()
                .tableId("test-table-id")
                .jobId(jobId)
                .partitionId("root")
                .inputFiles(List.of(UUID.randomUUID().toString()))
                .outputFile(UUID.randomUUID().toString()).build();
        JOBS_ON_QUEUE.add(job);
        return job;
    }

    private static MessageConsumer allJobsSucceed() {
        return (jobAndMessage) -> SUCCESSFUL_JOBS.add(jobAndMessage.getJob());
    }

    private static MessageConsumer withFailingJobs(CompactionJob... jobs) {
        Set<String> failingJobIds = Stream.of(jobs).map(CompactionJob::getId).collect(Collectors.toSet());
        return (jobAndMessage) -> {
            if (failingJobIds.contains(jobAndMessage.getJob().getId())) {
                throw new Exception("Failed to process job");
            } else {
                SUCCESSFUL_JOBS.add(jobAndMessage.getJob());
            }
        };
    }
}
