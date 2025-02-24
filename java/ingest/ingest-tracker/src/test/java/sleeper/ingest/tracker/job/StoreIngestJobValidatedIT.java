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

package sleeper.ingest.tracker.job;

import org.junit.jupiter.api.Test;

import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.tracker.testutils.DynamoDBIngestJobTrackerTestBase;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.acceptedRun;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.acceptedRunWhichStarted;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.ingestJobStatus;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.rejectedRun;
import static sleeper.ingest.core.job.IngestJobTestData.createJobWithTableAndFiles;

public class StoreIngestJobValidatedIT extends DynamoDBIngestJobTrackerTestBase {
    @Test
    void shouldReportUnstartedJobWithNoValidationFailures() {
        // Given
        IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
        Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

        // When
        tracker.jobValidated(job.acceptedEventBuilder(validationTime).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(ingestJobStatus(job, acceptedRun(job, validationTime)));
    }

    @Test
    void shouldReportStartedJobWithNoValidationFailures() {
        // Given
        String taskId = "some-task";
        IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
        Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
        Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");

        // When
        tracker.jobValidated(job.acceptedEventBuilder(validationTime).jobRunId("some-run").build());
        tracker.jobStarted(job.startedAfterValidationEventBuilder(startTime).taskId(taskId).jobRunId("some-run").build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(ingestJobStatus(job, acceptedRunWhichStarted(job, taskId,
                        validationTime, startTime)));
    }

    @Test
    void shouldReportJobWithOneValidationFailure() {
        // Given
        IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
        Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

        // When
        tracker.jobValidated(job.createRejectedEvent(validationTime, List.of("Test validation reason")));

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(ingestJobStatus(job, rejectedRun(
                        job, validationTime, "Test validation reason")));
    }

    @Test
    void shouldReportJobWithMultipleValidationFailures() {
        // Given
        IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
        Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

        // When
        tracker.jobValidated(job.createRejectedEvent(validationTime,
                List.of("Test validation reason 1", "Test validation reason 2")));

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(ingestJobStatus(job, rejectedRun(job, validationTime,
                        List.of("Test validation reason 1", "Test validation reason 2"))));
    }
}
