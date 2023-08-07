/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.compaction.status.store.job;

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.status.store.testutils.DynamoDBCompactionJobStatusStoreTestBase;
import sleeper.core.partition.Partition;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.statestore.FileInfoFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.record.process.status.TestRunStatusUpdates.defaultUpdateTime;

public class StoreCompactionJobExpiryIT extends DynamoDBCompactionJobStatusStoreTestBase {

    @Test
    public void shouldUpdateExpiryDateForCompactionJobStatusCreated() {
        // Given
        CompactionJob job = createCompactionJob();
        Duration timeToLive = Duration.ofDays(7);
        Instant createdTime = Instant.parse("2022-12-15T10:50:12.001Z");
        CompactionJobStatusStore store = storeWithTimeToLiveAndUpdateTimes(timeToLive, defaultUpdateTime(createdTime));

        // When
        store.jobCreated(job);

        // Then
        assertThat(getJobStatus(store, job.getId()).getExpiryDate())
                .isEqualTo(timePlusDurationAsExpiry(createdTime, timeToLive));
    }

    @Test
    public void shouldUpdateExpiryDateForCompactionJobStatusStarted() {
        // Given
        CompactionJob job = createCompactionJob();
        Duration timeToLive = Duration.ofDays(7);
        Instant createdTime = Instant.parse("2022-12-15T10:50:12.001Z");
        Instant startedTime = Instant.parse("2022-12-15T10:51:12.001Z");
        CompactionJobStatusStore store = storeWithTimeToLiveAndUpdateTimes(timeToLive,
                defaultUpdateTime(createdTime), defaultUpdateTime(startedTime));

        // When
        store.jobCreated(job);
        store.jobStarted(job, startedTime, DEFAULT_TASK_ID);

        // Then
        assertThat(getJobStatus(store, job.getId()).getExpiryDate())
                .isEqualTo(timePlusDurationAsExpiry(createdTime, timeToLive));
    }

    @Test
    public void shouldUpdateExpiryDateForCompactionJobStatusFinished() {
        // Given
        CompactionJob job = createCompactionJob();
        Duration timeToLive = Duration.ofDays(7);
        Instant createdTime = Instant.parse("2022-12-15T10:50:12.001Z");
        Instant startedTime = Instant.parse("2022-12-15T10:51:12.001Z");
        Instant finishedTime = Instant.parse("2022-12-15T10:52:12.001Z");
        CompactionJobStatusStore store = storeWithTimeToLiveAndUpdateTimes(timeToLive,
                defaultUpdateTime(createdTime), defaultUpdateTime(startedTime), defaultUpdateTime(finishedTime));

        // When
        store.jobCreated(job);
        store.jobStarted(job, startedTime, DEFAULT_TASK_ID);
        store.jobFinished(job, new RecordsProcessedSummary(
                new RecordsProcessed(60L, 60L), startedTime, finishedTime), DEFAULT_TASK_ID);

        // Then
        assertThat(getJobStatus(store, job.getId()).getExpiryDate())
                .isEqualTo(timePlusDurationAsExpiry(createdTime, timeToLive));
    }

    @Test
    public void shouldUpdateDifferentExpiryDateForCompactionJobStatusCreated() {
        // Given
        CompactionJob job = createCompactionJob();
        Duration timeToLive = Duration.ofDays(1);
        Instant createdTime = Instant.parse("2022-12-15T10:50:12.001Z");
        CompactionJobStatusStore store = storeWithTimeToLiveAndUpdateTimes(timeToLive, defaultUpdateTime(createdTime));

        // When
        store.jobCreated(job);

        // Then
        assertThat(getJobStatus(store, job.getId()).getExpiryDate())
                .isEqualTo(timePlusDurationAsExpiry(createdTime, timeToLive));
    }

    private CompactionJob createCompactionJob() {
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        return jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile("file1", 123L, "a", "c")),
                partition.getId());
    }

    private static Instant timePlusDurationAsExpiry(Instant time, Duration timeToLive) {
        return time.plus(timeToLive).with(ChronoField.MILLI_OF_SECOND, 0);
    }
}
