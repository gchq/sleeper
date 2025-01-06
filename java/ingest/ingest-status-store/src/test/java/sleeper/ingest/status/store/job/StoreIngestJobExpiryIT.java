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

import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.status.store.testutils.DynamoDBIngestJobTrackerTestBase;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoField;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.record.process.status.ProcessStatusUpdateTestHelper.defaultUpdateTime;

public class StoreIngestJobExpiryIT extends DynamoDBIngestJobTrackerTestBase {

    @Test
    public void shouldSetExpiryDateForStartedJob() {
        // Given
        IngestJob job = jobWithFiles("test-file");
        Instant startTime = Instant.parse("2022-12-15T11:32:42.001Z");
        Duration timeToLive = Duration.ofDays(7);

        IngestJobTracker tracker = trackerWithTimeToLiveAndUpdateTimes(timeToLive, defaultUpdateTime(startTime));
        tracker.jobStarted(defaultJobStartedEvent(job, startTime));

        // When/Then
        assertThat(getJobStatus(tracker, job.getId()).getExpiryDate())
                .isEqualTo(timePlusDurationAsExpiry(startTime, timeToLive));
    }

    @Test
    public void shouldSetExpiryDateForFinishedJob() {
        // Given
        IngestJob job = jobWithFiles("test-file");
        Instant startTime = Instant.parse("2022-12-15T11:32:42.001Z");
        Instant finishTime = Instant.parse("2022-12-15T11:33:42.001Z");
        Duration timeToLive = Duration.ofDays(7);

        IngestJobTracker tracker = trackerWithTimeToLiveAndUpdateTimes(timeToLive,
                defaultUpdateTime(startTime), defaultUpdateTime(finishTime));
        tracker.jobStarted(defaultJobStartedEvent(job, startTime));
        tracker.jobFinished(defaultJobFinishedEvent(job, startTime, finishTime));

        // When/Then
        assertThat(getJobStatus(tracker, job.getId()).getExpiryDate())
                .isEqualTo(timePlusDurationAsExpiry(startTime, timeToLive));
    }

    @Test
    public void shouldSetDifferentExpiryDateForStartedJob() {
        // Given
        IngestJob job = jobWithFiles("test-file");
        Instant startTime = Instant.parse("2022-12-15T11:32:42.001Z");
        Duration timeToLive = Duration.ofDays(1);

        IngestJobTracker tracker = trackerWithTimeToLiveAndUpdateTimes(timeToLive, defaultUpdateTime(startTime));
        tracker.jobStarted(defaultJobStartedEvent(job, startTime));

        // When/Then
        assertThat(getJobStatus(tracker, job.getId()).getExpiryDate())
                .isEqualTo(timePlusDurationAsExpiry(startTime, timeToLive));
    }

    private static Instant timePlusDurationAsExpiry(Instant time, Duration timeToLive) {
        return time.plus(timeToLive).with(ChronoField.MILLI_OF_SECOND, 0);
    }
}
