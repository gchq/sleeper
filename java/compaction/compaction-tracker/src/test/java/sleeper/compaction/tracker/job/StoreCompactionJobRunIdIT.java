/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.compaction.tracker.job;

import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobStatusFromJobTestData;
import sleeper.compaction.tracker.testutils.DynamoDBCompactionJobTrackerTestBase;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.FileReferenceFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionCommittedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionFinishedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.core.tracker.job.run.JobRunTestData.jobRunOnTask;

public class StoreCompactionJobRunIdIT extends DynamoDBCompactionJobTrackerTestBase {

    @Test
    public void shouldReportStartedJob() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(100L)),
                partition.getId());

        // When
        storeJobCreated(job);
        tracker.jobStarted(job.startedEventBuilder(defaultStartTime())
                .taskId(DEFAULT_TASK_ID).jobRunId("test-job-run").build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(startedStatusWithDefaults(job));
    }

    @Test
    void shouldReportUncommittedJob() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(100L)),
                partition.getId());

        // When
        storeJobCreated(job);
        tracker.jobStarted(job.startedEventBuilder(defaultStartTime())
                .taskId(DEFAULT_TASK_ID).jobRunId("test-job-run").build());
        tracker.jobFinished(job.finishedEventBuilder(defaultSummary())
                .taskId(DEFAULT_TASK_ID).jobRunId("test-job-run").build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(finishedUncommittedStatusWithDefaults(job));
    }

    @Test
    public void shouldReportFinishedAndCommittedJob() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(100L)),
                partition.getId());

        // When
        storeJobCreated(job);
        tracker.jobStarted(job.startedEventBuilder(defaultStartTime())
                .taskId(DEFAULT_TASK_ID).jobRunId("test-job-run").build());
        tracker.jobFinished(job.finishedEventBuilder(defaultSummary())
                .taskId(DEFAULT_TASK_ID).jobRunId("test-job-run").build());
        tracker.jobCommitted(job.committedEventBuilder(defaultCommitTime())
                .taskId(DEFAULT_TASK_ID).jobRunId("test-job-run").build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(finishedThenCommittedStatusWithDefaults(job));
    }

    @Test
    public void shouldReportFailedJob() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(100L)),
                partition.getId());

        // When
        storeJobCreated(job);
        tracker.jobStarted(job.startedEventBuilder(defaultStartTime())
                .taskId(DEFAULT_TASK_ID).jobRunId("test-job-run").build());
        tracker.jobFailed(job.failedEventBuilder(defaultFinishTime()).failure(new RuntimeException("Failed"))
                .taskId(DEFAULT_TASK_ID).jobRunId("test-job-run").build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(failedStatusWithDefaults(job, List.of("Failed")));
    }

    @Test
    public void shouldReportJobFinishedTwiceOnSameTaskThenCommittedTwice() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(100L)),
                partition.getId());

        // When
        storeJobCreated(job);
        tracker.jobStarted(job.startedEventBuilder(defaultStartTime())
                .taskId("test-task").jobRunId("test-run-1").build());
        tracker.jobFinished(job.finishedEventBuilder(defaultSummary())
                .taskId("test-task").jobRunId("test-run-1").build());
        tracker.jobStarted(job.startedEventBuilder(defaultStartTime())
                .taskId("test-task").jobRunId("test-run-2").build());
        tracker.jobFinished(job.finishedEventBuilder(defaultSummary())
                .taskId("test-task").jobRunId("test-run-2").build());
        tracker.jobCommitted(job.committedEventBuilder(defaultCommitTime())
                .taskId("test-task").jobRunId("test-run-1").build());
        tracker.jobCommitted(job.committedEventBuilder(defaultCommitTime())
                .taskId("test-task").jobRunId("test-run-2").build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(CompactionJobStatusFromJobTestData.compactionJobCreated(job, ignoredUpdateTime(),
                        jobRunOnTask("test-task",
                                compactionStartedStatus(defaultStartTime()),
                                compactionFinishedStatus(defaultSummary()),
                                compactionCommittedStatus(defaultCommitTime())),
                        jobRunOnTask("test-task",
                                compactionStartedStatus(defaultStartTime()),
                                compactionFinishedStatus(defaultSummary()),
                                compactionCommittedStatus(defaultCommitTime()))));
    }
}
