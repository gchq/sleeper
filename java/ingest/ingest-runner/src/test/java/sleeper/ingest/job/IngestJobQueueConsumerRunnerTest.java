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
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.task.IngestTaskStatusTestData.finishedNoJobs;
import static sleeper.ingest.task.IngestTaskStatusTestData.finishedOneJob;

public class IngestJobQueueConsumerRunnerTest {

    private final IngestJobHandler jobRunner = FixedIngestJobHandler.makingDefaultFiles();
    private final IngestTaskStatusStore statusStore = new WriteToMemoryIngestTaskStatusStore();

    @Test
    public void shouldRunAndReportTaskWithNoJobs() throws Exception {
        // Given
        String taskId = "test-task";
        Instant startTime = Instant.parse("2022-12-07T12:37:00.123Z");
        Instant finishTime = Instant.parse("2022-12-07T12:38:00.123Z");
        FixedIngestJobSource jobs = FixedIngestJobSource.empty();

        // When
        IngestJobQueueConsumerRunner runner = new IngestJobQueueConsumerRunner(jobs, taskId, statusStore,
                () -> startTime, () -> finishTime, Instant::now, Instant::now, jobRunner);
        runner.run();

        // Then
        assertThat(statusStore.getAllTasks()).containsExactly(finishedNoJobs(taskId, startTime, finishTime));
        assertThat(jobs.getIngestResults()).isEmpty();
    }

    @Test
    public void shouldRunAndReportTaskWithOneJob() throws Exception {
        // Given
        String taskId = "test-task";
        Instant startTaskTime = Instant.parse("2022-12-07T12:37:00.123Z");
        Instant finishTaskTime = Instant.parse("2022-12-07T12:38:00.123Z");
        Instant startJobTime = Instant.parse("2022-12-07T12:37:20.123Z");
        Instant finishJobTime = Instant.parse("2022-12-07T12:37:50.123Z");

        IngestJob job = IngestJob.builder()
                .id("test-job")
                .files(Collections.emptyList())
                .build();
        FixedIngestJobSource jobs = FixedIngestJobSource.with(job);

        // When
        IngestJobQueueConsumerRunner runner = new IngestJobQueueConsumerRunner(jobs, taskId, statusStore,
                () -> startTaskTime, () -> finishTaskTime, () -> startJobTime, () -> finishJobTime, jobRunner);
        runner.run();

        // Then
        assertThat(statusStore.getAllTasks()).containsExactly(
                finishedOneJob(taskId, startTaskTime, finishTaskTime, startJobTime, finishJobTime));
    }

}
