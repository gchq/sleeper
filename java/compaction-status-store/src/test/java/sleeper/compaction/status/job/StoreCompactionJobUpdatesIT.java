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
import sleeper.compaction.status.job.testutils.DynamoDBCompactionJobStatusStoreTestBase;
import sleeper.core.partition.Partition;
import sleeper.statestore.FileInfoFactory;

import java.time.Instant;
import java.util.Collections;

import static sleeper.compaction.status.job.testutils.AssertDynamoDBJobStatusRecord.createCompaction;
import static sleeper.compaction.status.job.testutils.AssertDynamoDBJobStatusRecord.finishCompaction;
import static sleeper.compaction.status.job.testutils.AssertDynamoDBJobStatusRecord.startCompaction;

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
        Instant startTime = Instant.parse("2022-09-22T11:09:12.001Z");
        store.jobStarted(job, startTime);

        // Then
        assertThatItemsInTable().containsExactlyInAnyOrder(
                createCompaction(job.getId(), 1, tableName, partition.getId()),
                startCompaction(job.getId(), startTime));
    }

    @Test
    public void shouldReportCompactionJobFinishedSeparatelyFromOthers() {
        // Given
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile(100L, "a", "z")),
                partition.getId());
        Instant startTime = Instant.parse("2022-09-22T11:09:12.001Z");
        Instant startTimeAtFinish = Instant.parse("2022-09-22T11:09:13.001Z");
        Instant finishTime = Instant.parse("2022-09-22T11:09:20.001Z");
        CompactionJobSummary summary = new CompactionJobSummary(
                new CompactionJobRecordsProcessed(200L, 100L),
                startTimeAtFinish, finishTime);

        // When
        store.jobCreated(job);
        store.jobStarted(job, startTime);
        store.jobFinished(job, summary);

        // Then
        assertThatItemsInTable().containsExactlyInAnyOrder(
                createCompaction(job.getId(), 1, tableName, partition.getId()),
                startCompaction(job.getId(), startTime),
                finishCompaction(job.getId(), summary));
    }

}
