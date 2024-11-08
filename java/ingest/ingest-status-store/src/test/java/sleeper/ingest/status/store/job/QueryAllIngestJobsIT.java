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

public class QueryAllIngestJobsIT extends DynamoDBIngestJobStatusStoreTestBase {

    @Test
    public void shouldReturnMultipleIngestJobsSortedMostRecentFirst() {
        // Given
        IngestJob job1 = jobWithFiles("file1");
        IngestJob job2 = jobWithFiles("file2");
        IngestJob job3 = jobWithFiles("file3");
        Instant startedTime1 = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant startedTime2 = Instant.parse("2022-12-14T13:52:12.001Z");
        Instant startedTime3 = Instant.parse("2022-12-14T13:53:12.001Z");

        // When
        store.jobStarted(defaultJobStartedEvent(job1, startedTime1));
        store.jobStarted(defaultJobStartedEvent(job2, startedTime2));
        store.jobStarted(defaultJobStartedEvent(job3, startedTime3));

        // Then
        assertThat(store.getAllJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(
                        defaultJobStartedStatus(job3, startedTime3),
                        defaultJobStartedStatus(job2, startedTime2),
                        defaultJobStartedStatus(job1, startedTime1));
    }

    @Test
    public void shouldExcludeIngestJobInOtherTable() {
        // Given
        IngestJob job1 = jobWithFiles("file1");
        IngestJob job2 = jobWithTableAndFiles("other-table", "file2");
        Instant startedTime1 = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant startedTime2 = Instant.parse("2022-12-14T13:52:12.001Z");

        // When
        store.jobStarted(defaultJobStartedEvent(job1, startedTime1));
        store.jobStarted(defaultJobStartedEvent(job2, startedTime2));

        // Then
        assertThat(store.getAllJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(defaultJobStartedStatus(job1, startedTime1));
    }

    @Test
    public void shouldReturnNoIngestJobs() {

        // When / Then
        assertThat(store.getAllJobs(tableId)).isEmpty();
    }
}
