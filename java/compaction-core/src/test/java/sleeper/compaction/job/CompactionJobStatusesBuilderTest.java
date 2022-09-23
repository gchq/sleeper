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
import sleeper.compaction.job.status.CompactionJobFinishedStatus;
import sleeper.compaction.job.status.CompactionJobStartedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.job.status.CompactionJobStatusesBuilder;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionJobStatusesBuilderTest {

    @Test
    public void shouldBuildCompactionJobStatusFromIndividualUpdates() {
        // Given
        CompactionJobStatusesBuilder builder = new CompactionJobStatusesBuilder()
                .jobCreated("job1", createdStatus1())
                .jobStarted("job1", startedStatus1())
                .jobFinished("job1", finishedStatus1())
                .jobCreated("job2", createdStatus2())
                .jobStarted("job2", startedStatus2())
                .jobFinished("job2", finishedStatus2());

        // When
        List<CompactionJobStatus> statuses = builder.build();

        // Then
        assertThat(statuses).containsExactly(
                CompactionJobStatus.builder().jobId("job1").createdStatus(createdStatus1())
                        .startedStatus(startedStatus1()).finishedStatus(finishedStatus1()).build(),
                CompactionJobStatus.builder().jobId("job2").createdStatus(createdStatus2())
                        .startedStatus(startedStatus2()).finishedStatus(finishedStatus2()).build());
    }

    @Test
    public void shouldGetJobIdsMissingCreatedRecord() {
        // Given
        CompactionJobStatusesBuilder builder = new CompactionJobStatusesBuilder()
                .jobCreated("job1", createdStatus1())
                .jobStarted("job2", startedStatus1())
                .jobFinished("job3", finishedStatus1());

        // When
        Set<String> jobsMissingCreatedStatus = builder.jobIdsMissingCreatedStatus();

        // When / Then
        assertThat(jobsMissingCreatedStatus).containsExactly("job2", "job3");
    }

    private CompactionJobCreatedStatus createdStatus1() {
        return CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
    }

    private CompactionJobStartedStatus startedStatus1() {
        return CompactionJobStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-23T09:23:30.012Z"),
                Instant.parse("2022-09-23T09:23:30.001Z"));
    }

    private CompactionJobFinishedStatus finishedStatus1() {
        return CompactionJobFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-23T09:24:00.012Z"),
                new CompactionJobSummary(new CompactionJobRecordsProcessed(200L, 100L),
                        Instant.parse("2022-09-23T09:23:30.001Z"),
                        Instant.parse("2022-09-23T09:24:00.001Z")));
    }

    private CompactionJobCreatedStatus createdStatus2() {
        return CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-24T09:23:00.012Z"))
                .partitionId("partition2").childPartitionIds(Arrays.asList("A", "B"))
                .inputFilesCount(12)
                .build();
    }

    private CompactionJobStartedStatus startedStatus2() {
        return CompactionJobStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
    }

    private CompactionJobFinishedStatus finishedStatus2() {
        return CompactionJobFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new CompactionJobSummary(new CompactionJobRecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")));
    }
}
