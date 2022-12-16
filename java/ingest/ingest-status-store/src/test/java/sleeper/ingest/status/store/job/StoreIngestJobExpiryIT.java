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

package sleeper.ingest.status.store.job;

import org.junit.Test;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.testutils.DynamoDBIngestJobStatusStoreTestBase;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoField;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.job.status.IngestJobStatusTestData.defaultUpdateTime;

public class StoreIngestJobExpiryIT extends DynamoDBIngestJobStatusStoreTestBase {

    @Test
    public void shouldSetExpiryDateForStartedJob() {
        // Given
        IngestJob job = jobWithFiles("test-file");
        Instant startTime = Instant.now();
        Duration timeToLive = Duration.ofDays(7);

        IngestJobStatusStore store = storeWithTimeToLiveAndUpdateTimes(timeToLive, defaultUpdateTime(startTime));
        store.jobStarted(DEFAULT_TASK_ID, job, startTime);

        // When/Then
        assertThat(getJobStatus(store, job.getId()).getExpiryDate())
                .isEqualTo(timePlusDurationAsExpiry(startTime, timeToLive));
    }

    @Test
    public void shouldSetExpiryDateForFinishedJob() {
        // Given
        IngestJob job = jobWithFiles("test-file");
        Instant startTime = Instant.now();
        Instant finishTime = startTime.plus(Duration.ofMinutes(1));
        Duration timeToLive = Duration.ofDays(7);

        IngestJobStatusStore store = storeWithTimeToLiveAndUpdateTimes(timeToLive,
                defaultUpdateTime(startTime), defaultUpdateTime(finishTime));
        store.jobStarted(DEFAULT_TASK_ID, job, startTime);
        store.jobFinished(DEFAULT_TASK_ID, job, defaultSummary(startTime, finishTime));

        // When/Then
        assertThat(getJobStatus(store, job.getId()).getExpiryDate())
                .isEqualTo(timePlusDurationAsExpiry(finishTime, timeToLive));
    }

    private static Instant timePlusDurationAsExpiry(Instant time, Duration timeToLive) {
        return time.plus(timeToLive).with(ChronoField.MILLI_OF_SECOND, 0);
    }
}
