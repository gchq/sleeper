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
package sleeper.ingest.task;

import org.junit.Test;
import sleeper.core.iterator.IteratorException;
import sleeper.ingest.IngestResult;
import sleeper.ingest.IngestResultTestData;
import sleeper.ingest.job.FixedIngestJobHandler;
import sleeper.ingest.job.FixedIngestJobSource;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobHandler;
import sleeper.ingest.job.IngestJobSource;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.job.status.WriteToMemoryIngestJobStatusStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.job.IngestJobTestData.DEFAULT_TABLE_NAME;
import static sleeper.ingest.job.IngestJobTestData.createJobInDefaultTable;
import static sleeper.ingest.task.IngestTaskStatusTestData.finishedMultipleJobs;
import static sleeper.ingest.task.IngestTaskStatusTestData.finishedNoJobs;
import static sleeper.ingest.task.IngestTaskStatusTestData.finishedOneJobNoFiles;
import static sleeper.ingest.task.IngestTaskStatusTestData.finishedOneJobOneFile;

public class IngestTaskTest {
    private final IngestJobHandler jobRunner = FixedIngestJobHandler.makingDefaultFiles();
    private final IngestTaskStatusStore taskStatusStore = new WriteToMemoryIngestTaskStatusStore();
    private final IngestJobStatusStore jobStatusStore = new WriteToMemoryIngestJobStatusStore();

    @Test
    public void shouldRunAndReportTaskWithNoJobs() throws Exception {
        // Given
        String taskId = "test-task";
        Instant startTime = Instant.parse("2022-12-07T12:37:00.123Z");
        Instant finishTime = Instant.parse("2022-12-07T12:38:00.123Z");
        FixedIngestJobSource jobs = FixedIngestJobSource.empty();

        // When
        runTask(jobs, taskId, timesInOrder(startTime, finishTime));

        // Then
        assertThat(taskStatusStore.getAllTasks()).containsExactly(finishedNoJobs(taskId, startTime, finishTime));
        assertThat(jobs.getIngestResults()).isEmpty();
        assertThat(jobStatusStore.getAllJobs(DEFAULT_TABLE_NAME)).isEmpty();
    }

    @Test
    public void shouldRunAndReportTaskWithOneJobAndNoFiles() throws Exception {
        // Given
        String taskId = "test-task";
        Instant startTaskTime = Instant.parse("2022-12-07T12:37:00.123Z");
        Instant finishTaskTime = Instant.parse("2022-12-07T12:38:00.123Z");
        Instant startJobTime = Instant.parse("2022-12-07T12:37:20.123Z");
        Instant finishJobTime = Instant.parse("2022-12-07T12:37:50.123Z");

        IngestJob job = createJobInDefaultTable("test-job");
        FixedIngestJobSource jobs = FixedIngestJobSource.with(job);

        // When
        runTask(jobs, taskId, timesInOrder(
                startTaskTime,
                startJobTime, finishJobTime,
                finishTaskTime));

        // Then
        assertThat(taskStatusStore.getAllTasks()).containsExactly(
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

        IngestJob job = createJobInDefaultTable("test-job", "test.parquet");
        FixedIngestJobSource jobs = FixedIngestJobSource.with(job);

        // When
        runTask(jobs, taskId, timesInOrder(
                startTaskTime,
                startJobTime, finishJobTime,
                finishTaskTime));

        // Then
        assertThat(taskStatusStore.getAllTasks()).containsExactly(
                finishedOneJobOneFile(taskId, startTaskTime, finishTaskTime, startJobTime, finishJobTime));
        assertThat(jobs.getIngestResults())
                .containsExactly(IngestResultTestData.defaultFileIngestResult("test.parquet"));
    }

    @Test
    public void shouldRunAndReportTaskWithMultipleJobs() throws Exception {
        // Given
        String taskId = "test-task";
        Instant startTaskTime = Instant.parse("2022-12-07T12:37:00.123Z");
        Instant finishTaskTime = Instant.parse("2022-12-07T12:38:00.123Z");
        Instant startJob1Time = Instant.parse("2022-12-07T12:37:10.123Z");
        Instant finishJob1Time = Instant.parse("2022-12-07T12:37:20.123Z");
        Instant startJob2Time = Instant.parse("2022-12-07T12:37:30.123Z");
        Instant finishJob2Time = Instant.parse("2022-12-07T12:37:40.123Z");

        IngestJob job1 = createJobInDefaultTable("test-job-1", "test1.parquet");
        IngestJob job2 = createJobInDefaultTable("test-job-2", "test2.parquet");
        FixedIngestJobSource jobs = FixedIngestJobSource.with(job1, job2);

        // When
        runTask(jobs, taskId, timesInOrder(
                startTaskTime,
                startJob1Time, finishJob1Time,
                startJob2Time, finishJob2Time,
                finishTaskTime));

        // Then
        assertThat(taskStatusStore.getAllTasks()).containsExactly(
                finishedMultipleJobs(taskId, startTaskTime, finishTaskTime, Duration.ofSeconds(10), startJob1Time, startJob2Time));
        assertThat(jobs.getIngestResults()).containsExactly(
                IngestResultTestData.defaultFileIngestResult("test1.parquet"),
                IngestResultTestData.defaultFileIngestResult("test2.parquet"));
    }

    private void runTask(IngestJobSource jobs, String taskId, Supplier<Instant> times)
            throws IteratorException, StateStoreException, IOException {
        IngestTask runner = new IngestTask(jobs, taskId, taskStatusStore, jobRunner, times);
        runner.run();
    }

    private static Supplier<Instant> timesInOrder(Instant... times) {
        return Arrays.asList(times).iterator()::next;
    }
}
