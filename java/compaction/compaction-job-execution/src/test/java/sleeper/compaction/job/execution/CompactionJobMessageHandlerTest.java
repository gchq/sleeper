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
import sleeper.compaction.job.execution.CompactionJobMessageHandler.JobAndMessage;
import sleeper.compaction.job.execution.CompactionJobMessageHandler.MessageConsumer;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES;

public class CompactionJobMessageHandlerTest {

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
            runJobMessageHandler(allJobsSucceed());

            // Then
            assertThat(SUCCESSFUL_JOBS).containsExactly(job);
            assertThat(FAILED_JOBS).isEmpty();
            assertThat(JOBS_ON_QUEUE).isEmpty();
        }

        @Test
        void shouldProcessFailingJob() throws Exception {
            // Given
            CompactionJob job = createJobOnQueue("job1");

            // When
            runJobMessageHandler(withFailingJobs(job));

            // Then
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
            runJobMessageHandler(withFailingJobs(job2));

            // Then=
            assertThat(SUCCESSFUL_JOBS).containsExactly(job1);
            assertThat(FAILED_JOBS).containsExactly(job2);
            assertThat(JOBS_ON_QUEUE).isEmpty();
        }
    }

    @Nested
    @DisplayName("Stop early if max consecutive failures met")
    class MaxConsecutiveFailures {
        @Test
        void shouldStopEarlyIfMaxConsecutiveFailuresMet() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES, 2);
            CompactionJob job1 = createJobOnQueue("job1");
            CompactionJob job2 = createJobOnQueue("job2");
            CompactionJob job3 = createJobOnQueue("job3");

            // When
            runJobMessageHandler(withFailingJobs(job1, job2));

            // Then
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
            runJobMessageHandler(withFailingJobs(job1, job3));

            // Then
            assertThat(SUCCESSFUL_JOBS).containsExactly(job2, job4);
            assertThat(FAILED_JOBS).containsExactly(job1, job3);
            assertThat(JOBS_ON_QUEUE).isEmpty();
        }
    }

    private InstanceProperties createInstance() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.setNumber(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES, 2);
        return instanceProperties;
    }

    private void runJobMessageHandler(MessageConsumer messageConsumer) throws Exception {
        new CompactionJobMessageHandler(instanceProperties, Instant::now, () -> {
            CompactionJob job = JOBS_ON_QUEUE.poll();
            if (job != null) {
                return Optional.of(new JobAndMessage(job, null));
            } else {
                return Optional.empty();
            }
        }, messageConsumer, (jobAndMessage) -> FAILED_JOBS.add(jobAndMessage.getJob())).run();
    }

    private CompactionJob createJobOnQueue(String jobId) {
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
