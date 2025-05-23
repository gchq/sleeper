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
import sleeper.compaction.tracker.testutils.DynamoDBCompactionJobTrackerTestBase;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.FileReferenceFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.core.job.CompactionJobStatusFromJobTestData.compactionJobCreated;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.startedCompactionRun;

public class QueryCompactionJobStatusByTaskIdIT extends DynamoDBCompactionJobTrackerTestBase {

    @Test
    public void shouldReturnCompactionJobsByTaskId() {
        // Given
        String searchingTaskId = "test-task";
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job1 = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile("file1", 123L)),
                partition.getId());
        CompactionJob job2 = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile("file2", 456L)),
                partition.getId());

        // When
        storeJobsCreated(job1, job2);
        tracker.jobStarted(job1.startedEventBuilder(defaultStartTime()).taskId(searchingTaskId).jobRunId("run1").build());
        tracker.jobStarted(job2.startedEventBuilder(defaultStartTime()).taskId("another-task").jobRunId("run2").build());

        // Then
        assertThat(tracker.getJobsByTaskId(tableId, searchingTaskId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(compactionJobCreated(job1, ignoredUpdateTime(),
                        startedCompactionRun(searchingTaskId, defaultStartTime())));
    }

    @Test
    public void shouldReturnCompactionJobByTaskIdInOneRun() {
        // Given
        String taskId1 = "task-id-1";
        String searchingTaskId = "test-task";
        String taskId3 = "task-id-3";
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile("file1", 123L)),
                partition.getId());

        // When
        storeJobCreated(job);
        tracker.jobStarted(job.startedEventBuilder(defaultStartTime()).taskId(taskId1).jobRunId("run1").build());
        tracker.jobStarted(job.startedEventBuilder(defaultStartTime()).taskId(searchingTaskId).jobRunId("run2").build());
        tracker.jobStarted(job.startedEventBuilder(defaultStartTime()).taskId(taskId3).jobRunId("run3").build());

        // Then
        assertThat(tracker.getJobsByTaskId(tableId, searchingTaskId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(compactionJobCreated(job, ignoredUpdateTime(),
                        startedCompactionRun(taskId3, defaultStartTime()),
                        startedCompactionRun(searchingTaskId, defaultStartTime()),
                        startedCompactionRun(taskId1, defaultStartTime())));
    }

    @Test
    public void shouldReturnNoCompactionJobsByTaskId() {
        // When / Then
        assertThat(tracker.getJobsByTaskId(tableId, "not-present")).isNullOrEmpty();
    }
}
