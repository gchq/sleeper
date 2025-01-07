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

package sleeper.ingest.status.store.job;

import org.junit.jupiter.api.Test;

import sleeper.core.tracker.ingest.job.update.IngestJobValidatedEvent;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.status.store.testutils.DynamoDBIngestJobTrackerTestBase;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestJobStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.rejectedRun;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.ingestJobStatus;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.rejectedRun;

public class QueryInvalidIngestJobsIT extends DynamoDBIngestJobTrackerTestBase {
    @Test
    public void shouldReturnInvalidIngestJobs() {
        // Given
        IngestJob job1 = jobWithFiles("file1");
        IngestJob job2 = jobWithFiles("file2");
        Instant validationTime1 = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant validationTime2 = Instant.parse("2022-12-14T13:52:12.001Z");

        // When
        tracker.jobValidated(job1.createRejectedEvent(validationTime1, List.of("Test reason 1")));
        tracker.jobValidated(job2.createRejectedEvent(validationTime2, List.of("Test reason 2")));

        // Then
        assertThat(tracker.getInvalidJobs())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(
                        ingestJobStatus(job2, rejectedRun(job2, validationTime2, "Test reason 2")),
                        ingestJobStatus(job1, rejectedRun(job1, validationTime1, "Test reason 1")));
    }

    @Test
    void shouldReturnInvalidIngestJobWhenTableIsUnknown() {
        // When
        String jobId = "invalid-job";
        String json = "{";
        Instant validationTime = Instant.parse("2023-11-06T10:36:00Z");
        tracker.jobValidated(IngestJobValidatedEvent.ingestJobRejected(jobId, json, validationTime, "Test reason"));

        // Then
        assertThat(tracker.getInvalidJobs())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(
                        ingestJobStatus(jobId, rejectedRun(jobId, json, validationTime, "Test reason")));
    }

    @Test
    public void shouldReturnInvalidIngestJobRejectedTwice() {
        // Given
        IngestJob job = jobWithFiles("file");
        Instant validationTime1 = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant validationTime2 = Instant.parse("2022-12-14T13:52:12.001Z");

        // When
        tracker.jobValidated(job.createRejectedEvent(validationTime1, List.of("Test reason 1")));
        tracker.jobValidated(job.createRejectedEvent(validationTime2, List.of("Test reason 2")));

        // Then
        assertThat(tracker.getInvalidJobs())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(
                        ingestJobStatus(job,
                                rejectedRun(job, validationTime2, "Test reason 2"),
                                rejectedRun(job, validationTime1, "Test reason 1")));
    }

    @Test
    public void shouldExcludeValidIngestJob() {
        // Given
        IngestJob job1 = jobWithFiles("file1");
        IngestJob job2 = jobWithFiles("file2");
        Instant validationTime1 = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant validationTime2 = Instant.parse("2022-12-14T13:52:12.001Z");

        // When
        tracker.jobValidated(job1.acceptedEventBuilder(validationTime1).build());
        tracker.jobValidated(job2.createRejectedEvent(validationTime2, List.of("Test reason 2")));

        // Then
        assertThat(tracker.getInvalidJobs())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(
                        ingestJobStatus(job2, rejectedRun(job2, validationTime2, "Test reason 2")));
    }

    @Test
    void shouldExcludeJobThatWasRejectedThenAccepted() {
        // Given
        IngestJob job = jobWithFiles("file1");
        Instant validationTime1 = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant validationTime2 = Instant.parse("2022-12-14T13:52:12.001Z");

        // When
        tracker.jobValidated(job.createRejectedEvent(validationTime1, List.of("Test reason 1")));
        tracker.jobValidated(job.acceptedEventBuilder(validationTime2).build());

        // Then
        assertThat(tracker.getInvalidJobs()).isEmpty();
    }
}
