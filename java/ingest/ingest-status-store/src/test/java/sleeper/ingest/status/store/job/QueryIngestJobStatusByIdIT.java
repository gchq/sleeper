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
package sleeper.ingest.status.store.job;

import org.junit.jupiter.api.Test;

import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.status.store.testutils.DynamoDBIngestJobStatusStoreTestBase;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryIngestJobStatusByIdIT extends DynamoDBIngestJobStatusStoreTestBase {

    @Test
    public void shouldReturnIngestJobById() {
        // Given
        IngestJob job1 = jobWithFiles("file1");
        IngestJob job2 = jobWithTableAndFiles("other-table", "file2");
        Instant startedTime1 = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant startedTime2 = Instant.parse("2022-12-14T13:52:12.001Z");

        // When
        tracker.jobStarted(defaultJobStartedEvent(job1, startedTime1));
        tracker.jobStarted(defaultJobStartedEvent(job2, startedTime2));

        // Then
        assertThat(getJobStatus(job1.getId()))
                .usingRecursiveComparison(IGNORE_UPDATE_TIMES)
                .isEqualTo(defaultJobStartedStatus(job1, startedTime1));
    }

    @Test
    public void shouldReturnFinishedIngestJobById() {
        // Given
        IngestJob job = jobWithFiles("file1");
        Instant startedTime = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant finishedTime = Instant.parse("2022-12-14T13:52:12.001Z");

        // When
        tracker.jobStarted(defaultJobStartedEvent(job, startedTime));
        tracker.jobFinished(defaultJobFinishedEvent(job, startedTime, finishedTime));

        // Then
        assertThat(getJobStatus(job.getId()))
                .usingRecursiveComparison(IGNORE_UPDATE_TIMES)
                .isEqualTo(defaultJobFinishedStatus(job, startedTime, finishedTime));
    }

    @Test
    public void shouldReturnNoIngestJobById() {
        // When / Then
        assertThat(tracker.getJob("not-present")).isNotPresent();
    }
}
