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
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobFinishedStatus;
import sleeper.compaction.job.status.CompactionJobStartedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.status.testutils.DynamoDBCompactionJobStatusStoreTestBase;
import sleeper.core.partition.Partition;
import sleeper.statestore.FileInfoFactory;

import java.time.Instant;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class StoreCompactionJobUpdatesIT extends DynamoDBCompactionJobStatusStoreTestBase {

    @Test
    public void shouldReportCompactionJobStartedSeparatelyFromCreation() {
        // Given
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile(100L, "a", "z")),
                partition.getId());

        // When
        store.jobCreated(job);
        store.jobStarted(job, defaultStartTime(), DEFAULT_TASK_ID);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(startedStatusWithDefaults(job));
    }

    @Test
    public void shouldReportCompactionJobFinishedSeparatelyFromOthers() {
        // Given
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile(100L, "a", "z")),
                partition.getId());

        // When
        store.jobCreated(job);
        store.jobStarted(job, defaultStartTime(), DEFAULT_TASK_ID);
        store.jobFinished(job, defaultSummary(), DEFAULT_TASK_ID);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(finishedStatusWithDefaults(job));
    }

    @Test
    public void shouldReportLatestUpdatesWhenJobIsRunMultipleTimes() {
        // Given
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile(100L, "a", "z")),
                partition.getId());
        Instant startTime1 = Instant.parse("2022-10-03T15:19:01.001Z");
        Instant finishTime1 = Instant.parse("2022-10-03T15:19:31.001Z");
        Instant startTime2 = Instant.parse("2022-10-03T15:19:02.001Z");
        Instant finishTime2 = Instant.parse("2022-10-03T15:19:32.001Z");
        CompactionJobRecordsProcessed processed = new CompactionJobRecordsProcessed(100L, 100L);

        // When
        store.jobCreated(job);
        store.jobStarted(job, startTime1, DEFAULT_TASK_ID);
        store.jobStarted(job, startTime2, DEFAULT_TASK_ID);
        store.jobFinished(job, new CompactionJobSummary(processed, startTime1, finishTime1), DEFAULT_TASK_ID);
        store.jobFinished(job, new CompactionJobSummary(processed, startTime2, finishTime2), DEFAULT_TASK_ID);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(CompactionJobStatus.builder().jobId(job.getId())
                        .createdStatus(CompactionJobCreatedStatus.from(
                                job, ignoredUpdateTime()))
                        .startedStatus(CompactionJobStartedStatus.updateAndStartTimeWithTaskId(
                                ignoredUpdateTime(), startTime2, DEFAULT_TASK_ID))
                        .finishedStatus(CompactionJobFinishedStatus.updateTimeAndSummary(
                                ignoredUpdateTime(), new CompactionJobSummary(processed, startTime2, finishTime2)))
                        .build());
    }

}
