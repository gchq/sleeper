/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.core.tracker.job.status;

import org.junit.jupiter.api.Test;

import sleeper.core.tracker.job.run.JobRun;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.finishedStatus;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.startedStatus;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.forJobRunOnTask;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.records;

public class JobStatusUpdatesGroupedByJobTest {

    @Test
    void shouldBuildJobStatusesInOrderOfInput() {
        // Given
        TestJobStartedStatus started1 = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));
        AggregatedTaskJobsFinishedStatus finished1 = finishedStatus(started1, Duration.ofSeconds(30), 200L, 100L);
        TestJobStartedStatus started2 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
        AggregatedTaskJobsFinishedStatus finished2 = finishedStatus(started2, Duration.ofSeconds(30), 450L, 300L);

        // When
        List<JobStatusUpdates> statuses = streamGroupedByJob(records().fromUpdates(
                forJobRunOnTask("job1", started1, finished1),
                forJobRunOnTask("job2", started2, finished2)).stream())
                .toList();

        // Then
        assertThat(statuses)
                .extracting(JobStatusUpdates::getJobId,
                        job -> job.getFirstRecord().getStatusUpdate(),
                        job -> job.getLastRecord().getStatusUpdate())
                .containsExactly(
                        tuple("job1", started1, finished1),
                        tuple("job2", started2, finished2));
    }

    @Test
    void shouldBuildJobStatusWithMultipleRuns() {
        // Given
        TestJobStartedStatus started1 = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));
        AggregatedTaskJobsFinishedStatus finished1 = finishedStatus(started1, Duration.ofSeconds(30), 200L, 100L);
        TestJobStartedStatus started2 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
        AggregatedTaskJobsFinishedStatus finished2 = finishedStatus(started2, Duration.ofSeconds(30), 450L, 300L);
        TestJobStartedStatus started3 = startedStatus(Instant.parse("2022-09-25T09:23:30.001Z"));

        // When
        List<JobStatusUpdates> statuses = streamGroupedByJob(records().fromUpdates(
                forJobRunOnTask("job1", "run1", "task1", started1, finished1),
                forJobRunOnTask("job1", "run2", "task2", started2, finished2),
                forJobRunOnTask("job2", "run3", "task1", started3)).stream())
                .toList();

        // Then
        assertThat(statuses)
                .extracting(JobStatusUpdates::getJobId,
                        job -> job.getRuns().getRunsLatestFirst().stream()
                                .map(JobRun::getStatusUpdates).toList())
                .containsExactly(
                        tuple("job1", List.of(List.of(started2, finished2), List.of(started1, finished1))),
                        tuple("job2", List.of(List.of(started3))));
    }

    @Test
    void shouldBuildNoJobStatusesFromNoRecords() {
        // When / Then
        assertThat(streamGroupedByJob(Stream.empty())).isEmpty();
    }

    @Test
    void shouldReturnJobMoreThanOnceWhenRecordsAreNotConsecutive() {
        // Given
        TestJobStartedStatus started1 = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));
        AggregatedTaskJobsFinishedStatus finished1 = finishedStatus(started1, Duration.ofSeconds(30), 200L, 100L);
        TestJobStartedStatus started2 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));

        // When
        List<JobStatusUpdates> statuses = streamGroupedByJob(records().fromUpdates(
                forJobRunOnTask("job1", "run1", "task1", started1),
                forJobRunOnTask("job2", "run2", "task1", started2),
                forJobRunOnTask("job1", "run1", "task1", finished1)).stream())
                .toList();

        // Then
        assertThat(statuses)
                .extracting(JobStatusUpdates::getJobId,
                        job -> job.getFirstRecord().getStatusUpdate(),
                        job -> job.getLastRecord().getStatusUpdate())
                .containsExactly(
                        tuple("job1", started1, started1),
                        tuple("job2", started2, started2),
                        tuple("job1", finished1, finished1));
    }

    @Test
    void shouldOnlyReadRecordsUpToStartOfNextJob() {
        // Given
        TestJobStartedStatus started1 = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));
        AggregatedTaskJobsFinishedStatus finished1 = finishedStatus(started1, Duration.ofSeconds(30), 200L, 100L);
        TestJobStartedStatus started2 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
        AggregatedTaskJobsFinishedStatus finished2 = finishedStatus(started2, Duration.ofSeconds(30), 450L, 300L);
        TestJobStartedStatus started3 = startedStatus(Instant.parse("2022-09-25T09:23:30.001Z"));
        List<String> readJobIds = new ArrayList<>();
        Stream<JobStatusUpdateRecord> records = records().fromUpdates(
                forJobRunOnTask("job1", started1, finished1),
                forJobRunOnTask("job2", started2, finished2),
                forJobRunOnTask("job3", started3)).stream()
                .peek(record -> readJobIds.add(record.getJobId()));

        // When
        List<String> jobIds = streamGroupedByJob(records)
                .limit(1)
                .map(JobStatusUpdates::getJobId)
                .toList();

        // Then
        assertThat(jobIds).containsExactly("job1");
        assertThat(readJobIds).containsExactly("job1", "job1", "job2");
    }

    @Test
    void shouldCloseUnderlyingStream() {
        // Given
        AtomicBoolean closed = new AtomicBoolean(false);
        Stream<JobStatusUpdateRecord> records = Stream.<JobStatusUpdateRecord>empty()
                .onClose(() -> closed.set(true));

        // When
        streamGroupedByJob(records).close();

        // Then
        assertThat(closed).isTrue();
    }

    private static Stream<JobStatusUpdates> streamGroupedByJob(Stream<JobStatusUpdateRecord> records) {
        return JobStatusUpdates.streamFromRecordsGroupedByJob(records);
    }
}
