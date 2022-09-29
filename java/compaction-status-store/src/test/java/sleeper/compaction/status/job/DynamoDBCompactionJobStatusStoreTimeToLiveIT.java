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

package sleeper.compaction.status.job;

import org.junit.Test;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobRecordsProcessed;
import sleeper.compaction.job.CompactionJobSummary;
import sleeper.compaction.status.testutils.DynamoDBCompactionJobStatusStoreTestBase;
import sleeper.core.partition.Partition;
import sleeper.statestore.FileInfoFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_JOB_STATUS_TTL_IN_SECONDS;

public class DynamoDBCompactionJobStatusStoreTimeToLiveIT extends DynamoDBCompactionJobStatusStoreTestBase {
    @Test
    public void shouldSetExpiryDateForCompactionJobStatusWithDefaultTimeToLive() {
        // Given
        CompactionJob job = createCompactionJob();

        // When
        Instant createdTime = Instant.now().plus(Duration.ofMillis(store.getTimeToLive()));
        store.jobCreated(job);

        // Then
        System.out.println(store.getJob(job.getId()));
        assertThat(store.getJob(job.getId()).getExpiryDate()).isAfterOrEqualTo(createdTime);
        assertThat(store.getTimeToLive())
                .isEqualTo(Long.parseLong(COMPACTION_JOB_STATUS_TTL_IN_SECONDS.getDefaultValue()) * 1000L);
    }

    @Test
    public void shouldSetExpiryDateForCompactionJobStatusWithCustomTimeToLive() {
        // Given
        long timeToLive = 10000L; // 10 seconds from creation time
        CompactionJob job = createCompactionJob();
        store.setTimeToLive(timeToLive);

        // When
        Instant createdTime = Instant.now().plus(Duration.ofMillis(store.getTimeToLive()));
        store.jobCreated(job);

        // Then
        assertThat(store.getJob(job.getId()).getExpiryDate()).isAfterOrEqualTo(createdTime);
        assertThat(store.getTimeToLive())
                .isEqualTo(timeToLive);
    }

    @Test
    public void shouldUpdateExpiryDateForCompactionJobStatusStarted() throws Exception {
        // Given
        CompactionJob job = createCompactionJob();

        // When
        store.jobCreated(job);
        Thread.sleep(2000L);

        Instant startedExpiryDate = Instant.now().plus(Duration.ofMillis(store.getTimeToLive()));
        Instant startedTime = Instant.now();
        store.jobStarted(job, startedTime);

        // Then
        assertThat(store.getJob(job.getId()).getExpiryDate()).isAfterOrEqualTo(startedExpiryDate);
        assertThat(store.getTimeToLive())
                .isEqualTo(Long.parseLong(COMPACTION_JOB_STATUS_TTL_IN_SECONDS.getDefaultValue()) * 1000L);
    }

    @Test
    public void shouldUpdateExpiryDateForCompactionJobStatusFinished() throws Exception {
        // Given
        CompactionJob job = createCompactionJob();

        // When
        store.jobCreated(job);
        Thread.sleep(2000L);

        Instant startedTime = Instant.now();
        store.jobStarted(job, startedTime);
        Thread.sleep(2000L);

        Instant finishedTime = Instant.now();
        Instant finishedExpiryDate = Instant.now().plus(Duration.ofMillis(store.getTimeToLive()));
        store.jobFinished(job, new CompactionJobSummary(
                new CompactionJobRecordsProcessed(60L, 60L), startedTime, finishedTime));
        
        // Then
        assertThat(store.getJob(job.getId()).getExpiryDate()).isAfterOrEqualTo(finishedExpiryDate);
        assertThat(store.getTimeToLive())
                .isEqualTo(Long.parseLong(COMPACTION_JOB_STATUS_TTL_IN_SECONDS.getDefaultValue()) * 1000L);
    }

    private CompactionJob createCompactionJob() {
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        return jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile("file1", 123L, "a", "c")),
                partition.getId());
    }
}
