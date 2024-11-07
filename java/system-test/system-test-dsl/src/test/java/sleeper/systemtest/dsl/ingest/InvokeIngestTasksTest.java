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
package sleeper.systemtest.dsl.ingest;

import org.junit.jupiter.api.Test;

import sleeper.core.util.PollWithRetries;
import sleeper.core.util.PollWithRetries.CheckFailedException;
import sleeper.ingest.core.job.status.InMemoryIngestJobStatusStore;
import sleeper.ingest.core.job.status.IngestJobStartedEvent;
import sleeper.ingest.core.job.status.IngestJobStatusStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class InvokeIngestTasksTest {

    private final IngestJobStatusStore jobStore = new InMemoryIngestJobStatusStore();
    private final List<Duration> waits = new ArrayList<>();
    private InvokeIngestTasks.InvokeTaskCreator invokeCreator;

    @Test
    void shouldInvokeTaskThatImmediatelyStartsJob() {
        // Given
        onInvokeTaskCreator(() -> {
            jobStore.jobStarted(jobStartedOnTask("test-job", "test-task", Instant.parse("2024-09-02T14:47:01Z")));
        });

        // When / Then
        assertThatCode(() -> invokeTasks()
                .invokeUntilNumTasksStartedAJob(1, List.of("test-job"), noRetries()))
                .doesNotThrowAnyException();
        assertThat(waits).isEmpty();
    }

    @Test
    void shouldPassWhenJobAlreadyStarted() {
        jobStore.jobStarted(jobStartedOnTask("test-job", "test-task", Instant.parse("2024-09-02T14:47:01Z")));

        // When / Then
        assertThatCode(() -> invokeTasks()
                .invokeUntilNumTasksStartedAJob(1, List.of("test-job"), noRetries()))
                .doesNotThrowAnyException();
        assertThat(waits).isEmpty();
    }

    @Test
    void shouldPassWithMoreTasksThanExpected() {
        jobStore.jobStarted(jobStartedOnTask("job-1", "task-1", Instant.parse("2024-09-02T14:47:01Z")));
        jobStore.jobStarted(jobStartedOnTask("job-2", "task-2", Instant.parse("2024-09-02T14:47:02Z")));

        // When / Then
        assertThatCode(() -> invokeTasks()
                .invokeUntilNumTasksStartedAJob(1, List.of("job-1", "job-2"), noRetries()))
                .doesNotThrowAnyException();
        assertThat(waits).isEmpty();
    }

    @Test
    void shouldFailToStartAJob() {
        // Given
        onInvokeTaskCreator(() -> {
            // Do nothing
        });

        // When / Then
        assertThatThrownBy(() -> invokeTasks()
                .invokeUntilNumTasksStartedAJob(1, List.of("test-job"), noRetries()))
                .isInstanceOf(CheckFailedException.class);
        assertThat(waits).isEmpty();
    }

    @Test
    void shouldStartJobOnRetry() {
        // Given
        onInvokeTaskCreator(() -> {
            // Do nothing on first invocation
        }, () -> {
            jobStore.jobStarted(jobStartedOnTask("test-job", "test-task", Instant.parse("2024-09-02T14:47:01Z")));
        });

        // When / Then
        assertThatCode(() -> invokeTasks()
                .invokeUntilNumTasksStartedAJob(1, List.of("test-job"), retries(1)))
                .doesNotThrowAnyException();
        assertThat(waits).hasSize(1);
    }

    @Test
    void shouldPassWithMoreTasksThanExpectedOnRetry() {
        onInvokeTaskCreator(() -> {
            // Do nothing on first invocation
        }, () -> {
            jobStore.jobStarted(jobStartedOnTask("job-1", "task-1", Instant.parse("2024-09-02T14:47:01Z")));
            jobStore.jobStarted(jobStartedOnTask("job-2", "task-2", Instant.parse("2024-09-02T14:47:02Z")));
        });

        // When / Then
        assertThatCode(() -> invokeTasks()
                .invokeUntilNumTasksStartedAJob(1, List.of("job-1", "job-2"), retries(1)))
                .doesNotThrowAnyException();
        assertThat(waits).hasSize(1);
    }

    @Test
    void shouldFailToStartEnoughTasks() {
        // Given
        onInvokeTaskCreator(() -> {
            jobStore.jobStarted(jobStartedOnTask("job-1", "test-task", Instant.parse("2024-09-02T14:47:01Z")));
            jobStore.jobStarted(jobStartedOnTask("job-2", "test-task", Instant.parse("2024-09-02T14:47:02Z")));
        });

        // When / Then
        assertThatThrownBy(() -> invokeTasks()
                .invokeUntilNumTasksStartedAJob(2, List.of("job-1", "job-2"), noRetries()))
                .isInstanceOf(CheckFailedException.class);
        assertThat(waits).isEmpty();
    }

    @Test
    void shouldFailWhenTaskStartedWithUnexpectedJob() {
        // Given
        onInvokeTaskCreator(() -> {
            jobStore.jobStarted(jobStartedOnTask("other-job", "test-task", Instant.parse("2024-09-02T14:47:01Z")));
        });

        // When / Then
        assertThatThrownBy(() -> invokeTasks()
                .invokeUntilNumTasksStartedAJob(2, List.of("test-job"), noRetries()))
                .isInstanceOf(CheckFailedException.class);
        assertThat(waits).isEmpty();
    }

    @Test
    void shouldFailWhenNoJobsGiven() {
        // When / Then
        assertThatThrownBy(() -> invokeTasks()
                .invokeUntilNumTasksStartedAJob(1, List.of(), noRetries()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(waits).isEmpty();
    }

    private InvokeIngestTasks invokeTasks() {
        return new InvokeIngestTasks(invokeCreator, jobStore);
    }

    private PollWithRetries noRetries() {
        return retries(0);
    }

    private PollWithRetries retries(int retries) {
        return PollWithRetries.builder()
                .pollIntervalMillis(10)
                .maxRetries(retries)
                .sleepInInterval(millis -> waits.add(Duration.ofMillis(millis)))
                .build();
    }

    private void onInvokeTaskCreator(Runnable... actions) {
        Iterator<Runnable> iterator = List.of(actions).iterator();
        invokeCreator = () -> iterator.next().run();
    }

    private IngestJobStartedEvent jobStartedOnTask(String jobId, String taskId, Instant startTime) {
        return IngestJobStartedEvent.builder()
                .jobId(jobId)
                .taskId(taskId)
                .tableId("test-table")
                .startTime(startTime)
                .startOfRun(true)
                .build();
    }

}
