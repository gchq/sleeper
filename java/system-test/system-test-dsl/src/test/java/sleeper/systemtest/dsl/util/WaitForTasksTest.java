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
package sleeper.systemtest.dsl.util;

import org.junit.jupiter.api.Test;

import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.InMemoryCompactionJobTracker;
import sleeper.core.tracker.compaction.job.update.CompactionJobStartedEvent;
import sleeper.core.tracker.ingest.job.InMemoryIngestJobTracker;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.update.IngestJobStartedEvent;
import sleeper.core.util.PollWithRetries;
import sleeper.core.util.PollWithRetries.CheckFailedException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class WaitForTasksTest {

    private final IngestJobTracker ingestJobTracker = new InMemoryIngestJobTracker();
    private final CompactionJobTracker compactionJobTracker = new InMemoryCompactionJobTracker();
    private final List<Duration> waits = new ArrayList<>();
    private Runnable onWait = () -> {
    };

    @Test
    void shouldFindIngestStartsDuringFirstWait() {
        // Given
        onWait(() -> {
            ingestJobTracker.jobStarted(ingestJobStartedOnTask("test-job", "test-task", Instant.parse("2024-09-02T14:47:01Z")));
        });

        // When / Then
        assertThatCode(() -> waitUntilNumTasksStartedAnIngest(1, List.of("test-job"), retries(1)))
                .doesNotThrowAnyException();
        assertThat(waits).hasSize(1);
    }

    @Test
    void shouldPassWhenIngestAlreadyStarted() {
        ingestJobTracker.jobStarted(ingestJobStartedOnTask("test-job", "test-task", Instant.parse("2024-09-02T14:47:01Z")));

        // When / Then
        assertThatCode(() -> waitUntilNumTasksStartedAnIngest(1, List.of("test-job"), noRetries()))
                .doesNotThrowAnyException();
        assertThat(waits).isEmpty();
    }

    @Test
    void shouldPassWithMoreTasksThanExpected() {
        ingestJobTracker.jobStarted(ingestJobStartedOnTask("job-1", "task-1", Instant.parse("2024-09-02T14:47:01Z")));
        ingestJobTracker.jobStarted(ingestJobStartedOnTask("job-2", "task-2", Instant.parse("2024-09-02T14:47:02Z")));

        // When / Then
        assertThatCode(() -> waitUntilNumTasksStartedAnIngest(1, List.of("job-1", "job-2"), noRetries()))
                .doesNotThrowAnyException();
        assertThat(waits).isEmpty();
    }

    @Test
    void shouldFailToStartAJob() {
        // Given
        onWait(() -> {
            // Do nothing
        });

        // When / Then
        assertThatThrownBy(() -> waitUntilNumTasksStartedAnIngest(1, List.of("test-job"), noRetries()))
                .isInstanceOf(CheckFailedException.class);
        assertThat(waits).isEmpty();
    }

    @Test
    void shouldStartIngestOnRetry() {
        // Given
        onWait(() -> {
            // Do nothing on first wait
        }, () -> {
            ingestJobTracker.jobStarted(ingestJobStartedOnTask("test-job", "test-task", Instant.parse("2024-09-02T14:47:01Z")));
        });

        // When / Then
        assertThatCode(() -> waitUntilNumTasksStartedAnIngest(1, List.of("test-job"), retries(2)))
                .doesNotThrowAnyException();
        assertThat(waits).hasSize(2);
    }

    @Test
    void shouldPassWithMoreTasksThanExpectedOnRetry() {
        onWait(() -> {
            // Do nothing on first wait
        }, () -> {
            ingestJobTracker.jobStarted(ingestJobStartedOnTask("job-1", "task-1", Instant.parse("2024-09-02T14:47:01Z")));
            ingestJobTracker.jobStarted(ingestJobStartedOnTask("job-2", "task-2", Instant.parse("2024-09-02T14:47:02Z")));
        });

        // When / Then
        assertThatCode(() -> waitUntilNumTasksStartedAnIngest(1, List.of("job-1", "job-2"), retries(2)))
                .doesNotThrowAnyException();
        assertThat(waits).hasSize(2);
    }

    @Test
    void shouldFailToStartEnoughIngestTasks() {
        // Given
        onWait(() -> {
            ingestJobTracker.jobStarted(ingestJobStartedOnTask("job-1", "test-task", Instant.parse("2024-09-02T14:47:01Z")));
            ingestJobTracker.jobStarted(ingestJobStartedOnTask("job-2", "test-task", Instant.parse("2024-09-02T14:47:02Z")));
        });

        // When / Then
        assertThatThrownBy(() -> waitUntilNumTasksStartedAnIngest(2, List.of("job-1", "job-2"), noRetries()))
                .isInstanceOf(CheckFailedException.class);
        assertThat(waits).isEmpty();
    }

    @Test
    void shouldFailWhenTaskStartedWithUnexpectedJob() {
        // Given
        onWait(() -> {
            ingestJobTracker.jobStarted(ingestJobStartedOnTask("other-job", "test-task", Instant.parse("2024-09-02T14:47:01Z")));
        });

        // When / Then
        assertThatThrownBy(() -> waitUntilNumTasksStartedAnIngest(2, List.of("test-job"), noRetries()))
                .isInstanceOf(CheckFailedException.class);
        assertThat(waits).isEmpty();
    }

    @Test
    void shouldFailWhenNoJobsGiven() {
        // When / Then
        assertThatThrownBy(() -> waitUntilNumTasksStartedAnIngest(1, List.of(), noRetries()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(waits).isEmpty();
    }

    @Test
    void shouldFindCompactionStartsDuringFirstWait() {
        // Given
        onWait(() -> {
            compactionJobTracker.jobStarted(compactionJobStartedOnTask("test-job", "test-task", Instant.parse("2024-09-02T14:47:01Z")));
        });

        // When / Then
        assertThatCode(() -> waitUntilNumTasksStartedACompaction(1, List.of("test-job"), retries(1)))
                .doesNotThrowAnyException();
        assertThat(waits).hasSize(1);
    }

    @Test
    void shouldFailToStartEnoughCompactionTasks() {
        // Given
        onWait(() -> {
            compactionJobTracker.jobStarted(compactionJobStartedOnTask("job-1", "test-task", Instant.parse("2024-09-02T14:47:01Z")));
            compactionJobTracker.jobStarted(compactionJobStartedOnTask("job-2", "test-task", Instant.parse("2024-09-02T14:47:02Z")));
        });

        // When / Then
        assertThatThrownBy(() -> waitUntilNumTasksStartedAnIngest(2, List.of("job-1", "job-2"), noRetries()))
                .isInstanceOf(CheckFailedException.class);
        assertThat(waits).isEmpty();
    }

    private void waitUntilNumTasksStartedAnIngest(int expectedTasks, List<String> jobIds, PollWithRetries poll) {
        new WaitForTasks(ingestJobTracker).waitUntilNumTasksStartedAJob(expectedTasks, jobIds, poll);
    }

    private void waitUntilNumTasksStartedACompaction(int expectedTasks, List<String> jobIds, PollWithRetries poll) {
        new WaitForTasks(compactionJobTracker).waitUntilNumTasksStartedAJob(expectedTasks, jobIds, poll);
    }

    private PollWithRetries noRetries() {
        return retries(0);
    }

    private PollWithRetries retries(int retries) {
        return PollWithRetries.builder()
                .pollIntervalMillis(10)
                .maxRetries(retries)
                .sleepInInterval(millis -> {
                    waits.add(Duration.ofMillis(millis));
                    onWait.run();
                })
                .build();
    }

    private void onWait(Runnable... actions) {
        Iterator<Runnable> iterator = List.of(actions).iterator();
        onWait = () -> iterator.next().run();
    }

    private IngestJobStartedEvent ingestJobStartedOnTask(String jobId, String taskId, Instant startTime) {
        return IngestJobStartedEvent.builder()
                .jobId(jobId)
                .taskId(taskId)
                .jobRunId(UUID.randomUUID().toString())
                .tableId("test-table")
                .startTime(startTime)
                .build();
    }

    private CompactionJobStartedEvent compactionJobStartedOnTask(String jobId, String taskId, Instant startTime) {
        return CompactionJobStartedEvent.builder()
                .jobId(jobId)
                .taskId(taskId)
                .jobRunId(UUID.randomUUID().toString())
                .tableId("test-table")
                .startTime(startTime)
                .build();
    }

}
