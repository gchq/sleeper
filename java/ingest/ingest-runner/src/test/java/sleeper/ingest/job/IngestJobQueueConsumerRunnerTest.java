/*
 * Copyright 2022 Crown Copyright
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
package sleeper.ingest.job;

import org.junit.Test;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.ingest.task.WriteToMemoryIngestTaskStatusStore;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static sleeper.ingest.job.IngestJobSource.Callback;
import static sleeper.ingest.task.TestIngestTaskStatus.finishedNoJobs;

public class IngestJobQueueConsumerRunnerTest {

    private final IngestJobQueueConsumer queueConsumer = mock(IngestJobQueueConsumer.class);
    private final IngestTaskStatusStore statusStore = new WriteToMemoryIngestTaskStatusStore();
    private final IngestJobRunner jobRunner = mock(IngestJobRunner.class);

    @Test
    public void shouldRunAndReportTaskWithNoJobs() throws Exception {
        // Given
        String taskId = "test-task";
        Instant startTime = Instant.parse("2022-12-07T12:37:00.123Z");
        Instant finishTime = Instant.parse("2022-12-07T12:38:00.123Z");
        Callback callback = jobRunner::ingest;

        // When
        IngestJobQueueConsumerRunner runner = new IngestJobQueueConsumerRunner(queueConsumer, taskId, statusStore,
                () -> startTime, () -> finishTime, Instant::now, Instant::now, callback);
        runner.run();

        // Then
        assertThat(statusStore.getAllTasks()).containsExactly(finishedNoJobs(taskId, startTime, finishTime));
        verify(queueConsumer).consumeJobs(callback);
    }
/*
    @Test
    public void shouldRunAndReportTaskWithOneJob() throws Exception {
        // Given
        String taskId = "test-task";
        Instant startTaskTime = Instant.parse("2022-12-07T12:37:00.123Z");
        Instant finishTaskTime = Instant.parse("2022-12-07T12:38:00.123Z");
        Instant startJobTime = Instant.parse("2022-12-07T12:37:20.123Z");
        Instant finishJobTime = Instant.parse("2022-12-07T12:37:50.123Z");
        Callback callback = jobRunner::ingest;

        IngestJob job = IngestJob.builder()
                .id("test-job")
                .files(Collections.emptyList())
                .build();
        when(queueConsumer.waitForMessage()).thenReturn(Optional.of(IngestJobProviderResult.from(job, "receipt")));

        // When
        IngestJobQueueConsumerRunner runner = new IngestJobQueueConsumerRunner(queueConsumer, taskId, statusStore,
                () -> startTaskTime, () -> finishTaskTime, () -> startJobTime, () -> finishJobTime, callback);
        runner.run();

        // Then
        assertThat(statusStore.getAllTasks()).containsExactly(finishedOneJob(taskId, startTaskTime, finishTaskTime, startJobTime, finishJobTime));
        verify(queueConsumer).consumeJobs(callback);
    }
 */
}
