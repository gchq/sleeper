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
package sleeper.compaction.job;

import org.junit.Test;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.job.status.CompactionJobStatusUpdateRecord;
import sleeper.compaction.job.status.CompactionJobStatusesBuilder;
import sleeper.compaction.job.status.ProcessRun;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.status.ProcessFinishedStatus;
import sleeper.core.status.ProcessStartedStatus;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobTestDataHelper.DEFAULT_TASK_ID;

public class CompactionJobStatusesBuilderTest {

    @Test
    public void shouldBuildCompactionJobStatusFromIndividualUpdates() {
        // Given
        CompactionJobCreatedStatus created1 = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-23T09:23:30.012Z"),
                Instant.parse("2022-09-23T09:23:30.001Z"));
        ProcessFinishedStatus finished1 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-23T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(200L, 100L),
                        Instant.parse("2022-09-23T09:23:30.001Z"),
                        Instant.parse("2022-09-23T09:24:00.001Z")));
        CompactionJobCreatedStatus created2 = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-24T09:23:00.012Z"))
                .partitionId("partition2").childPartitionIds(Arrays.asList("A", "B"))
                .inputFilesCount(12)
                .build();
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished2 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")));

        List<CompactionJobStatusUpdateRecord> updates = new TestCompactionJobStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID, created1, started1, finished1)
                .updatesForJobWithTask("job2", DEFAULT_TASK_ID, created2, started2, finished2)
                .list();

        // When
        List<CompactionJobStatus> statuses = new CompactionJobStatusesBuilder().jobUpdates(updates).build();

        // Then
        assertThat(statuses).containsExactly(
                CompactionJobStatus.builder().jobId("job2").createdStatus(created2)
                        .singleJobRun(ProcessRun.finished(DEFAULT_TASK_ID, started2, finished2)).build(),
                CompactionJobStatus.builder().jobId("job1").createdStatus(created1)
                        .singleJobRun(ProcessRun.finished(DEFAULT_TASK_ID, started1, finished1)).build());
    }
}
