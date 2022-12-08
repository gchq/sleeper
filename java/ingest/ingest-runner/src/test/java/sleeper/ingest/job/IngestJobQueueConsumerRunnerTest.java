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
import sleeper.ingest.IngestResult;
import sleeper.ingest.IngestResultTestData;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.ingest.task.WriteToMemoryIngestTaskStatusStore;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.task.IngestTaskStatusTestData.finishedNoJobs;
import static sleeper.ingest.task.IngestTaskStatusTestData.finishedOneJobNoFiles;
import static sleeper.ingest.task.IngestTaskStatusTestData.finishedOneJobOneFile;

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
        IngestJobQueueConsumerRunner runner = new IngestJobQueueConsumerRunner(jobs, taskId, statusStore, jobRunner,
                timesInOrder(startTime, finishTime));
        runner.run();

        // Then
        assertThat(statusStore.getAllTasks()).containsExactly(finishedNoJobs(taskId, startTime, finishTime));
        assertThat(jobs.getIngestResults()).isEmpty();
    }

    @Test
    public void shouldRunAndReportTaskWithOneJobAndNoFiles() throws Exception {
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
        IngestJobQueueConsumerRunner runner = new IngestJobQueueConsumerRunner(jobs, taskId, statusStore, jobRunner,
                timesInOrder(startTaskTime, startJobTime, finishJobTime, finishTaskTime));
        runner.run();

        // Then
        assertThat(statusStore.getAllTasks()).containsExactly(
                finishedOneJobNoFiles(taskId, startTaskTime, finishTaskTime, startJobTime, finishJobTime));
        assertThat(jobs.getIngestResults()).containsExactly(
                IngestResult.from(Collections.emptyList()));
    }

    @Test
    public void shouldRunAndReportTaskWithOneJobAndOneFile() throws Exception {
        // Given
        String taskId = "test-task";
        Instant startTaskTime = Instant.parse("2022-12-07T12:37:00.123Z");
        Instant finishTaskTime = Instant.parse("2022-12-07T12:38:00.123Z");
        Instant startJobTime = Instant.parse("2022-12-07T12:37:20.123Z");
        Instant finishJobTime = Instant.parse("2022-12-07T12:37:50.123Z");

        IngestJob job = IngestJob.builder()
                .id("test-job")
                .files(Collections.singletonList("test.parquet"))
                .build();
        FixedIngestJobSource jobs = FixedIngestJobSource.with(job);

        // When
        IngestJobQueueConsumerRunner runner = new IngestJobQueueConsumerRunner(jobs, taskId, statusStore, jobRunner,
                timesInOrder(startTaskTime, startJobTime, finishJobTime, finishTaskTime));
        runner.run();

        // Then
        assertThat(statusStore.getAllTasks()).containsExactly(
                finishedOneJobOneFile(taskId, startTaskTime, finishTaskTime, startJobTime, finishJobTime));
        assertThat(jobs.getIngestResults())
                .containsExactly(IngestResultTestData.defaultFileIngestResult("test.parquet"));
    }

    private static Supplier<Instant> timesInOrder(Instant... times) {
        return Arrays.asList(times).iterator()::next;
    }
}
