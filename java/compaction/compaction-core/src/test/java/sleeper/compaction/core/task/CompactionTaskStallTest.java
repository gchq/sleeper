/*
 * Copyright 2022-2026 Crown Copyright
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionRequest;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.compaction.core.task.CompactionTask.MessageHandle;
import sleeper.core.testutils.TestInstantSupplier;
import sleeper.core.tracker.compaction.task.CompactionTaskFinishedStatus;
import sleeper.core.tracker.compaction.task.CompactionTaskStatus;
import sleeper.core.tracker.job.run.JobRunTime;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.core.util.ThreadSleep;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.core.job.CompactionJobStatusFromJobTestData.compactionJobCreated;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_PROGRESS_CHECK_PERIOD_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_PROGRESS_FIRST_UPDATE_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_PROGRESS_STALL_TIMEOUT_IN_SECONDS;
import static sleeper.core.testutils.SupplierTestHelper.supplyTimes;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.failedCompactionRun;

public class CompactionTaskStallTest extends CompactionTaskTestBase {

    private static final Instant TASK_START = Instant.parse("2026-10-26T09:59:00Z");
    private static final Instant JOB_START = Instant.parse("2026-10-26T10:00:00Z");

    private final CountDownLatch compactionStarted = new CountDownLatch(1);
    private final CountDownLatch terminated = new CountDownLatch(1);

    @BeforeEach
    void setUp() {
        instanceProperties.setNumber(COMPACTION_TASK_PROGRESS_FIRST_UPDATE_TIMEOUT_IN_SECONDS, 600);
        instanceProperties.setNumber(COMPACTION_TASK_PROGRESS_STALL_TIMEOUT_IN_SECONDS, 300);
        instanceProperties.setNumber(COMPACTION_TASK_PROGRESS_CHECK_PERIOD_IN_SECONDS, 30);
        terminator = reason -> {
            terminations.add(reason);
            terminated.countDown();
        };
    }

    @Test
    void shouldTerminateWhenNoProgressUpdateReceived() throws Exception {
        // Given
        CompactionJob job = createJobOnQueue("job1");
        TestInstantSupplier watchdogTimes = supplyTimes(
                JOB_START, // Watchdog started
                Instant.parse("2026-10-26T10:10:00Z")); // Check finds stall
        setWatchdog(watchdogTimes, checkAfterCompactionStarted());

        // When
        runTask(compactionHangsAfterUpdates(), taskTimes());

        // Then
        Instant stallTime = Instant.parse("2026-10-26T10:10:00Z");
        String reason = "No progress update received in 10m 0s since compaction started, exceeding maximum of 10m 0s";
        assertThat(terminations).containsExactly("Compaction job job1 stalled: " + reason);
        assertThat(jobsReturnedToQueue).containsExactly(job);
        assertThat(consumedJobs).isEmpty();
        assertThat(stateStoreCommitQueue).isEmpty();
        assertThat(jobTracker.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                compactionJobCreated(job, DEFAULT_CREATED_TIME,
                        failedCompactionRun(DEFAULT_TASK_ID, new JobRunTime(JOB_START, stallTime),
                                List.of("Compaction stalled: " + reason))));
        assertThat(taskTracker.getAllTasks()).containsExactly(taskFinishedAt(stallTime));
        assertThat(watchdogTimes.getRemainingTimes()).isEmpty();
    }

    @Test
    void shouldTerminateWhenRowsProcessedUnchanged() throws Exception {
        // Given
        CompactionJob job = createJobOnQueue("job1");
        TestInstantSupplier watchdogTimes = supplyTimes(
                JOB_START, // Watchdog started
                Instant.parse("2026-10-26T10:01:00Z"), // Progress update
                Instant.parse("2026-10-26T10:02:00Z"), // Progress update, unchanged
                Instant.parse("2026-10-26T10:06:00Z")); // Check finds stall
        setWatchdog(watchdogTimes, checkAfterCompactionStarted());

        // When
        runTask(compactionHangsAfterUpdates(100, 100), taskTimes());

        // Then
        Instant stallTime = Instant.parse("2026-10-26T10:06:00Z");
        String reason = "Rows processed unchanged at 100 for 5m 0s, exceeding maximum of 5m 0s. " +
                "Last progress update received 4m 0s ago";
        assertThat(terminations).containsExactly("Compaction job job1 stalled: " + reason);
        assertThat(jobsReturnedToQueue).containsExactly(job);
        assertThat(consumedJobs).isEmpty();
        assertThat(jobTracker.getAllJobs(DEFAULT_TABLE_ID)).containsExactly(
                compactionJobCreated(job, DEFAULT_CREATED_TIME,
                        failedCompactionRun(DEFAULT_TASK_ID, new JobRunTime(JOB_START, stallTime),
                                List.of("Compaction stalled: " + reason))));
        assertThat(taskTracker.getAllTasks()).containsExactly(taskFinishedAt(stallTime));
        assertThat(watchdogTimes.getRemainingTimes()).isEmpty();
    }

    @Test
    void shouldNotTerminateWhenCompactionFinishesBeforeStall() throws Exception {
        // Given
        CompactionJob job = createJobOnQueue("job1");
        TestInstantSupplier watchdogTimes = supplyTimes(
                JOB_START, // Watchdog started
                Instant.parse("2026-10-26T10:01:00Z"), // Progress update
                Instant.parse("2026-10-26T10:02:00Z")); // Progress update
        setWatchdog(watchdogTimes, waitUntilInterrupted());

        // When
        runTask(compactionSucceedsWithUpdates(100, 200));

        // Then
        assertThat(terminations).isEmpty();
        assertThat(consumedJobs).containsExactly(job);
        assertThat(jobsReturnedToQueue).isEmpty();
        assertThat(watchdogTimes.getRemainingTimes()).isEmpty();
    }

    @Test
    void shouldTerminateWhenReturningJobToQueueFails() throws Exception {
        // Given
        CompactionJob job = createJob("job1");
        setWatchdog(supplyTimes(JOB_START, Instant.parse("2026-10-26T10:10:00Z")), checkAfterCompactionStarted());

        // When
        runTask(receiveOnce(messageFailingToReturnToQueue(job)), compactionHangsAfterUpdates(), taskTimes());

        // Then
        assertThat(terminations).hasSize(1);
        assertThat(taskTracker.getAllTasks()).containsExactly(
                taskFinishedAt(Instant.parse("2026-10-26T10:10:00Z")));
    }

    private TestInstantSupplier taskTimes() {
        return supplyTimes(
                TASK_START, // Task start
                TASK_START, // Max alive time check
                JOB_START); // Job start
    }

    private CompactionTaskStatus taskFinishedAt(Instant finishTime) {
        return CompactionTaskStatus.builder()
                .taskId(DEFAULT_TASK_ID)
                .startTime(TASK_START)
                .finished(finishTime, CompactionTaskFinishedStatus.builder())
                .build();
    }

    /**
     * Simulates a compaction stuck in native code. Sends the given progress updates, then blocks until the task is
     * terminated. In practice the JVM would exit, but here the compaction returns so the test can finish.
     *
     * @param  rowsProcessedUpdates the progress updates to send before blocking
     * @return                      the compaction runner
     */
    private CompactionRunner compactionHangsAfterUpdates(long... rowsProcessedUpdates) {
        return monitoredRunner(request -> {
            for (long rows : rowsProcessedUpdates) {
                request.getProgressCallback().accept(rows);
            }
            compactionStarted.countDown();
            assertThat(terminated.await(10, TimeUnit.SECONDS))
                    .as("compaction task should be terminated")
                    .isTrue();
            return new RowsProcessed(10, 10);
        });
    }

    private CompactionRunner compactionSucceedsWithUpdates(long... rowsProcessedUpdates) {
        return monitoredRunner(request -> {
            for (long rows : rowsProcessedUpdates) {
                request.getProgressCallback().accept(rows);
            }
            return new RowsProcessed(10, 10);
        });
    }

    private ThreadSleep checkAfterCompactionStarted() {
        return millis -> compactionStarted.await();
    }

    private ThreadSleep waitUntilInterrupted() {
        return millis -> new CountDownLatch(1).await();
    }

    private CompactionTask.MessageReceiver receiveOnce(MessageHandle message) {
        return pollQueue(() -> Optional.of(message));
    }

    private MessageHandle messageFailingToReturnToQueue(CompactionJob job) {
        return new MessageHandle() {
            @Override
            public CompactionJob getJob() {
                return job;
            }

            @Override
            public void deleteFromQueue() {
                throw new IllegalStateException("Unexpected delete from queue");
            }

            @Override
            public void returnToQueue() {
                throw new IllegalStateException("Failed returning to queue");
            }

            @Override
            public void close() {
            }
        };
    }

    private static CompactionRunner monitoredRunner(MonitoredCompaction compaction) {
        return new CompactionRunner() {
            @Override
            public RowsProcessed compact(CompactionRequest request) {
                try {
                    return compaction.run(request);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean isProgressMonitored() {
                return true;
            }
        };
    }

    /**
     * A fake compaction for a runner whose progress is monitored.
     */
    @FunctionalInterface
    private interface MonitoredCompaction {
        RowsProcessed run(CompactionRequest request) throws InterruptedException;
    }
}
