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
    }

    private void runTask(IngestRunner compactor) throws Exception {
        runTask(compactor, Instant::now);
    }

    private void runTask(IngestRunner compactor, Supplier<Instant> timeSupplier) throws Exception {
        runTask(pollQueue(), compactor, timeSupplier, DEFAULT_TASK_ID);
    }

    private void runTask(
            MessageReceiver messageReceiver,
            IngestRunner compactor,
            Supplier<Instant> timeSupplier,
            String taskId) throws Exception {
        new NewIngestTask(timeSupplier, messageReceiver, compactor, taskStore, taskId)
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

    private ProcessJob jobSucceeds() {
        return new ProcessJob(true, 10L, DEFAULT_START_TIME, DEFAULT_DURATION);
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
