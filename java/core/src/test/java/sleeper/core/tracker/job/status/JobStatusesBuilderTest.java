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
package sleeper.core.tracker.job.status;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.finishedStatus;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.startedStatus;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.forJobRunOnTask;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.records;

public class JobStatusesBuilderTest {

    @Test
    public void shouldBuildJobStatusesFromIndividualUpdates() {
        // Given
        TestJobStartedStatus started1 = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));
        AggregatedTaskJobsFinishedStatus finished1 = finishedStatus(started1, Duration.ofSeconds(30), 200L, 100L);
        TestJobStartedStatus started2 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
        AggregatedTaskJobsFinishedStatus finished2 = finishedStatus(started2, Duration.ofSeconds(30), 450L, 300L);

        // When
        List<JobStatusUpdates> statuses = jobStatusListFrom(records().fromUpdates(
                forJobRunOnTask("job1", started1, finished1),
                forJobRunOnTask("job2", started2, finished2)));

        // Then
        assertThat(statuses)
                .extracting(JobStatusUpdates::getJobId,
                        job -> job.getFirstRecord().getStatusUpdate(),
                        job -> job.getLastRecord().getStatusUpdate())
                .containsExactly(
                        tuple("job2", started2, finished2),
                        tuple("job1", started1, finished1));
    }

    private static List<JobStatusUpdates> jobStatusListFrom(TestJobStatusUpdateRecords records) {
        return JobStatusUpdates.streamFrom(records.stream()).collect(Collectors.toList());
    }
}
