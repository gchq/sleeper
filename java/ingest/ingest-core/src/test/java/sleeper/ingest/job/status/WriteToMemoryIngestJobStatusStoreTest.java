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
package sleeper.ingest.job.status;

import org.junit.Test;
import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.job.status.IngestJobStatusTestData.jobStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestData.startedIngestRun;

public class WriteToMemoryIngestJobStatusStoreTest {

    private final WriteToMemoryIngestJobStatusStore store = new WriteToMemoryIngestJobStatusStore();

    @Test
    public void shouldReturnOneStartedJobWithNoFiles() {
        String tableName = "test-table";
        String taskId = "test-task";
        Instant startTime = Instant.parse("2022-09-22T12:00:14.000Z");
        IngestJob job = IngestJob.builder()
                .id("test-job")
                .tableName(tableName)
                .files(Collections.emptyList())
                .build();

        store.jobStarted(taskId, job, startTime);
        assertThat(store.getAllJobs(tableName)).containsExactly(
                jobStatus(job, startedIngestRun(job, taskId, startTime, startTime)));
    }

    @Test
    public void shouldReturnOneStartedJobWithFiles() {
        String tableName = "test-table";
        String taskId = "test-task";
        Instant startTime = Instant.parse("2022-09-22T12:00:14.000Z");
        IngestJob job = IngestJob.builder()
                .id("test-job")
                .tableName(tableName)
                .files("test-file-1.parquet", "test-file-2.parquet")
                .build();

        store.jobStarted(taskId, job, startTime);
        assertThat(store.getAllJobs(tableName)).containsExactly(
                jobStatus(job, startedIngestRun(job, taskId, startTime, startTime)));
    }
}
