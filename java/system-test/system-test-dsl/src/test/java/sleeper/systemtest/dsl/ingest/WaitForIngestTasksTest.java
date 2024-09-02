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
import sleeper.ingest.job.status.InMemoryIngestJobStatusStore;
import sleeper.ingest.job.status.IngestJobStartedEvent;
import sleeper.ingest.job.status.IngestJobStatusStore;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class WaitForIngestTasksTest {

    private final IngestJobStatusStore jobStore = new InMemoryIngestJobStatusStore();
    private InvokeIngestTasksDriverNew invokeDriver;

    @Test
    void shouldInvokeTaskThatImmediatelyStartsJob() {
        // Given
        onInvokeTaskCreator(() -> {
            jobStore.jobStarted(jobStartedOnTask("test-job", "test-task", Instant.parse("2024-09-02T14:47:01Z")));
        });

        // When / Then
        assertThatCode(() -> waitForTasks()
                .invokeUntilNumTasksStartedAJob(1, List.of("test-job"), PollWithRetries.noRetries()))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldFailToStartAJob() {
        // Given
        onInvokeTaskCreator(() -> {
            // Do nothing
        });

        // When / Then
        assertThatThrownBy(() -> waitForTasks()
                .invokeUntilNumTasksStartedAJob(1, List.of("test-job"), PollWithRetries.noRetries()))
                .isInstanceOf(CheckFailedException.class);
    }

    @Test
    void shouldStartJobOnRetry() {
        // Given
        Iterator<Runnable> attempts = List.<Runnable>of(() -> {
            // Do nothing
        }, () -> {
            jobStore.jobStarted(jobStartedOnTask("test-job", "test-task", Instant.parse("2024-09-02T14:47:01Z")));
        }).iterator();
        onInvokeTaskCreator(() -> attempts.next().run());

        // When / Then
        assertThatCode(() -> waitForTasks()
                .invokeUntilNumTasksStartedAJob(1, List.of("test-job"), PollWithRetries.immediateRetries(1)))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldFailToStartEnoughTasks() {
        // Given
        onInvokeTaskCreator(() -> {
            jobStore.jobStarted(jobStartedOnTask("job-1", "test-task", Instant.parse("2024-09-02T14:47:01Z")));
            jobStore.jobStarted(jobStartedOnTask("job-2", "test-task", Instant.parse("2024-09-02T14:47:02Z")));
        });

        // When / Then
        assertThatThrownBy(() -> waitForTasks()
                .invokeUntilNumTasksStartedAJob(2, List.of("job-1", "job-2"), PollWithRetries.noRetries()))
                .isInstanceOf(CheckFailedException.class);
    }

    @Test
    void shouldStartTaskWithUnexpectedJob() {
        // Given
        onInvokeTaskCreator(() -> {
            jobStore.jobStarted(jobStartedOnTask("other-job", "test-task", Instant.parse("2024-09-02T14:47:01Z")));
        });

        // When / Then
        assertThatThrownBy(() -> waitForTasks()
                .invokeUntilNumTasksStartedAJob(2, List.of("test-job"), PollWithRetries.noRetries()))
                .isInstanceOf(CheckFailedException.class);
    }

    private WaitForIngestTasks waitForTasks() {
        return new WaitForIngestTasks(invokeDriver, jobStore);
    }

    private void onInvokeTaskCreator(Runnable action) {
        invokeDriver = action::run;
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
