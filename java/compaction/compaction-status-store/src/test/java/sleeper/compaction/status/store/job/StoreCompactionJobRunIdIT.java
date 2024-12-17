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
package sleeper.compaction.status.store.job;

import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.status.store.testutils.DynamoDBCompactionJobStatusStoreTestBase;
import sleeper.core.partition.Partition;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.statestore.FileReferenceFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.compactionCommittedStatus;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.compactionFinishedStatus;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.jobCreated;

public class StoreCompactionJobRunIdIT extends DynamoDBCompactionJobStatusStoreTestBase {

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
        store.jobStarted(job.startedEventBuilder(defaultStartTime())
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
        store.jobStarted(job.startedEventBuilder(defaultStartTime())
                .taskId(DEFAULT_TASK_ID).jobRunId("test-job-run").build());
        store.jobFinished(job.finishedEventBuilder(defaultSummary())
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
        store.jobStarted(job.startedEventBuilder(defaultStartTime())
                .taskId(DEFAULT_TASK_ID).jobRunId("test-job-run").build());
        store.jobFinished(job.finishedEventBuilder(defaultSummary())
                .taskId(DEFAULT_TASK_ID).jobRunId("test-job-run").build());
        store.jobCommitted(job.committedEventBuilder(defaultCommitTime())
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
        store.jobStarted(job.startedEventBuilder(defaultStartTime())
                .taskId(DEFAULT_TASK_ID).jobRunId("test-job-run").build());
        store.jobFailed(job.failedEventBuilder(defaultRunTime()).failure(new RuntimeException("Failed"))
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
        store.jobStarted(job.startedEventBuilder(defaultStartTime())
                .taskId("test-task").jobRunId("test-run-1").build());
        store.jobFinished(job.finishedEventBuilder(defaultSummary())
                .taskId("test-task").jobRunId("test-run-1").build());
        store.jobStarted(job.startedEventBuilder(defaultStartTime())
                .taskId("test-task").jobRunId("test-run-2").build());
        store.jobFinished(job.finishedEventBuilder(defaultSummary())
                .taskId("test-task").jobRunId("test-run-2").build());
        store.jobCommitted(job.committedEventBuilder(defaultCommitTime())
                .taskId("test-task").jobRunId("test-run-1").build());
        store.jobCommitted(job.committedEventBuilder(defaultCommitTime())
                .taskId("test-task").jobRunId("test-run-2").build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobCreated(job, ignoredUpdateTime(),
                        ProcessRun.builder().taskId("test-task")
                                .startedStatus(compactionStartedStatus(defaultStartTime()))
                                .finishedStatus(compactionFinishedStatus(defaultSummary()))
                                .statusUpdate(compactionCommittedStatus(defaultCommitTime()))
                                .build(),
                        ProcessRun.builder().taskId("test-task")
                                .startedStatus(compactionStartedStatus(defaultStartTime()))
                                .finishedStatus(compactionFinishedStatus(defaultSummary()))
                                .statusUpdate(compactionCommittedStatus(defaultCommitTime()))
                                .build()));
    }
}
