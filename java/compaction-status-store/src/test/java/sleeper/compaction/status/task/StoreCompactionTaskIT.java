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

package sleeper.compaction.status.task;

import org.junit.Test;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobRecordsProcessed;
import sleeper.compaction.job.CompactionJobSummary;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobFinishedStatus;
import sleeper.compaction.job.status.CompactionJobStartedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.status.testutils.DynamoDBCompactionTaskStatusStoreTestBase;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.core.partition.Partition;
import sleeper.statestore.FileInfoFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class StoreCompactionTaskIT extends DynamoDBCompactionTaskStatusStoreTestBase {
    @Test
    public void shouldReportCompactionTaskStarted() {
        //Given
        CompactionTaskStatus taskStatus = startedTaskWithDefaults();

        //When
        store.taskStarted(taskStatus);

        //Then
        assertThat(store.getTask(taskStatus.getTaskId()))
                .usingRecursiveComparison(IGNORE_FINISHED_STATUS)
                .isEqualTo(taskStatus);
    }

    @Test
    public void shouldReportCompactionTaskFinished() {
        //Given
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        CompactionJob job1 = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile(100L, "a", "z")),
                partition.getId());

        //When
        CompactionTaskStatus taskStatus = finishedTaskWithDefaults(createJobStatus(job1));
        store.taskStarted(taskStatus);
        store.taskFinished(taskStatus);

        //Then
        assertThat(store.getTask(taskStatus.getTaskId()))
                .usingRecursiveComparison(IGNORE_UPDATE_TIMES)
                .isEqualTo(taskStatus);
    }

    @Test
    public void shouldReportSplittingCompactionTaskFinished() {
        //Given
        CompactionJob job1 = singleFileSplittingCompaction("C", "A", "B");

        //When
        CompactionTaskStatus taskStatus = finishedTaskWithDefaults(createJobStatus(job1));
        store.taskStarted(taskStatus);
        store.taskFinished(taskStatus);

        //Then
        assertThat(store.getTask(taskStatus.getTaskId()))
                .usingRecursiveComparison(IGNORE_UPDATE_TIMES)
                .isEqualTo(taskStatus);
    }

    @Test
    public void shouldReportNoCompactionTaskExistsInStore() {
        //Given
        CompactionTaskStatus taskStatus = startedTaskWithDefaults();

        // When/Then
        assertThat(store.getTask(taskStatus.getTaskId()))
                .usingRecursiveComparison(IGNORE_UPDATE_TIMES)
                .isNull();
    }

    private List<CompactionJobStatus> createJobStatus(CompactionJob job1) {
        Instant jobCreationTime = Instant.parse("2022-09-22T14:00:00.000Z");
        Instant jobStartedTime = Instant.parse("2022-09-22T14:00:02.000Z");
        Instant jobStartedUpdateTime = Instant.parse("2022-09-22T14:00:04.000Z");
        Instant jobFinishTime = Instant.parse("2022-09-22T14:00:14.000Z");

        CompactionJobSummary summary = new CompactionJobSummary(
                new CompactionJobRecordsProcessed(4800L, 2400L), jobStartedUpdateTime, jobFinishTime);

        CompactionJobStatus status = CompactionJobStatus.builder().jobId(job1.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job1, jobCreationTime))
                .startedStatus(CompactionJobStartedStatus.updateAndStartTime(
                        jobStartedUpdateTime, jobStartedTime))
                .finishedStatus(CompactionJobFinishedStatus.updateTimeAndSummary(jobStartedUpdateTime, summary))
                .build();

        return Collections.singletonList(status);
    }
}
