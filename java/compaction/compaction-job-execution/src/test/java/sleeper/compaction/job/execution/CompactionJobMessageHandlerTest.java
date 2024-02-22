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
    private static final Queue<CompactionJob> jobsOnQueue = new LinkedList<>();
    private static final List<CompactionJob> successfulJobs = new ArrayList<>();
    private static final List<CompactionJob> failedJobs = new ArrayList<>();

    @BeforeEach
    void setUp() {
        jobsOnQueue.clear();
        successfulJobs.clear();
        failedJobs.clear();
    }

    @Nested
    @DisplayName("Process jobs")
    class ProcessJobs {
        @Test
        void shouldProcessSuccessfulJob() throws Exception {
            // Given
            CompactionJob job = createJob("job1");
            jobsOnQueue.add(job);

            // When
            runJobMessageHandler(allJobsSucceed());

            // Then
            assertThat(successfulJobs).containsExactly(job);
            assertThat(failedJobs).isEmpty();
            assertThat(jobsOnQueue).isEmpty();
        }

        @Test
        void shouldProcessFailingJob() throws Exception {
            // Given
            CompactionJob job = createJob("job1");
            jobsOnQueue.add(job);

            // When
            runJobMessageHandler(withFailingJobs(job));

            // Then
            assertThat(successfulJobs).isEmpty();
            assertThat(failedJobs).containsExactly(job);
            assertThat(jobsOnQueue).isEmpty();
        }

        @Test
        void shouldProcessSuccessfulThenFailingJob() throws Exception {
            // Given
            CompactionJob job1 = createJob("job1");
            jobsOnQueue.add(job1);
            CompactionJob job2 = createJob("job2");
            jobsOnQueue.add(job2);

            // When
            runJobMessageHandler(withFailingJobs(job2));

            // Then=
            assertThat(successfulJobs).containsExactly(job1);
            assertThat(failedJobs).containsExactly(job2);
            assertThat(jobsOnQueue).isEmpty();
        }
    }

    @Nested
    @DisplayName("Stop early if max consecutive failures met")
    class MaxConsecutiveFailures {
        @Test
        void shouldStopEarlyIfMaxConsecutiveFailuresMet() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES, 2);
            CompactionJob job1 = createJob("job1");
            jobsOnQueue.add(job1);
            CompactionJob job2 = createJob("job2");
            jobsOnQueue.add(job2);
            CompactionJob job3 = createJob("job3");
            jobsOnQueue.add(job3);

            // When
            runJobMessageHandler(withFailingJobs(job1, job2));

            // Then
            assertThat(successfulJobs).isEmpty();
            assertThat(failedJobs).containsExactly(job1, job2);
            assertThat(jobsOnQueue).containsExactly(job3);
        }

        @Test
        void shouldResetConsecutiveFailureCountIfJobProcessedSuccessfully() throws Exception {
            // Given
            instanceProperties.setNumber(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES, 2);
            CompactionJob job1 = createJob("job1");
            jobsOnQueue.add(job1);
            CompactionJob job2 = createJob("job2");
            jobsOnQueue.add(job2);
            CompactionJob job3 = createJob("job3");
            jobsOnQueue.add(job3);
            CompactionJob job4 = createJob("job4");
            jobsOnQueue.add(job4);

            // When
            runJobMessageHandler(withFailingJobs(job1, job3));

            // Then
            assertThat(successfulJobs).containsExactly(job2, job4);
            assertThat(failedJobs).containsExactly(job1, job3);
            assertThat(jobsOnQueue).isEmpty();
        }
    }

    private InstanceProperties createInstance() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.setNumber(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES, 2);
        return instanceProperties;
    }

    private void runJobMessageHandler(MessageConsumer messageConsumer) throws Exception {
        new CompactionJobMessageHandler(instanceProperties, Instant::now, () -> {
            CompactionJob job = jobsOnQueue.poll();
            if (job != null) {
                return Optional.of(new JobAndMessage(job, null));
            } else {
                return Optional.empty();
            }
        }, messageConsumer, (jobAndMessage) -> failedJobs.add(jobAndMessage.getJob())).run();
    }

    private CompactionJob createJob(String jobId) {
        return CompactionJob.builder()
                .tableId("test-table-id")
                .jobId(jobId)
                .partitionId("root")
                .inputFiles(List.of(UUID.randomUUID().toString()))
                .outputFile(UUID.randomUUID().toString()).build();
    }

    private static MessageConsumer allJobsSucceed() {
        return (jobAndMessage) -> successfulJobs.add(jobAndMessage.getJob());
    }

    private static MessageConsumer withFailingJobs(CompactionJob... jobs) {
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
