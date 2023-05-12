/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.systemtest.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.util.PollWithRetries;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.testutils.CompactionJobStatusStoreInMemory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.record.process.RecordsProcessedSummaryTestData.summary;
import static sleeper.job.common.QueueMessageCountsInMemory.singleQueueVisibleMessages;

class WaitForQueueEstimateTest {

    InstanceProperties properties = createTestInstanceProperties();

    @Nested
    @DisplayName("Wait for non-empty queue")
    class WaitForNonEmptyQueue {

        @Test
        void shouldTimeOutWaitingForNonEmptyQueue() {
            // Given
            properties.set(SPLITTING_COMPACTION_JOB_QUEUE_URL, "test-job-queue");
            WaitForQueueEstimate wait = WaitForQueueEstimate.notEmpty(
                    singleQueueVisibleMessages("test-job-queue", 0),
                    properties, SPLITTING_COMPACTION_JOB_QUEUE_URL);
            PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(0, 1);

            // When / Then
            assertThatThrownBy(() -> wait.pollUntilFinished(poll))
                    .isInstanceOf(PollWithRetries.TimedOutException.class);
        }

        @Test
        void shouldFinishWaitingForNonEmptyQueue() {
            // Given
            properties.set(SPLITTING_COMPACTION_JOB_QUEUE_URL, "test-job-queue");
            WaitForQueueEstimate wait = WaitForQueueEstimate.notEmpty(
                    singleQueueVisibleMessages("test-job-queue", 1),
                    properties, SPLITTING_COMPACTION_JOB_QUEUE_URL);
            PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(0, 1);

            // When / Then
            assertThatCode(() -> wait.pollUntilFinished(poll))
                    .doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("Wait for empty queue")
    class WaitForEmptyQueue {

        @Test
        void shouldTimeOutWaitingForEmptyQueue() {
            // Given
            properties.set(INGEST_JOB_QUEUE_URL, "test-job-queue");
            WaitForQueueEstimate wait = WaitForQueueEstimate.isEmpty(
                    singleQueueVisibleMessages("test-job-queue", 1),
                    properties, INGEST_JOB_QUEUE_URL);
            PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(0, 1);

            // When / Then
            assertThatThrownBy(() -> wait.pollUntilFinished(poll))
                    .isInstanceOf(PollWithRetries.TimedOutException.class);
        }

        @Test
        void shouldFinishWaitingForEmptyQueue() {
            // Given
            properties.set(INGEST_JOB_QUEUE_URL, "test-job-queue");
            WaitForQueueEstimate wait = WaitForQueueEstimate.isEmpty(
                    singleQueueVisibleMessages("test-job-queue", 0),
                    properties, INGEST_JOB_QUEUE_URL);
            PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(0, 1);

            // When / Then
            assertThatCode(() -> wait.pollUntilFinished(poll))
                    .doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("Wait for queue to match count of unfinished compaction jobs")
    class WaitForQueueToMatchUnfinishedCompactionJobs {

        String tableName = "test-table";
        CompactionJobTestDataHelper jobHelper = CompactionJobTestDataHelper.forTable(tableName);
        CompactionJobStatusStore statusStore = new CompactionJobStatusStoreInMemory();

        @Test
        void shouldTimeOutWaitingForQueueEstimateToIncludeSingleUnfinishedJob() {
            // Given
            statusStore.jobCreated(jobHelper.singleFileCompaction());

            properties.set(INGEST_JOB_QUEUE_URL, "test-job-queue");
            WaitForQueueEstimate wait = WaitForQueueEstimate.containsUnfinishedJobs(
                    singleQueueVisibleMessages("test-job-queue", 0),
                    properties, INGEST_JOB_QUEUE_URL, statusStore, tableName);
            PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(0, 1);

            // When / Then
            assertThatThrownBy(() -> wait.pollUntilFinished(poll))
                    .isInstanceOf(PollWithRetries.TimedOutException.class);
        }

        @Test
        void shouldFinishWaitingForQueueEstimateToIncludeSingleUnfinishedJob() {
            // Given
            statusStore.jobCreated(jobHelper.singleFileCompaction());

            properties.set(INGEST_JOB_QUEUE_URL, "test-job-queue");
            WaitForQueueEstimate wait = WaitForQueueEstimate.containsUnfinishedJobs(
                    singleQueueVisibleMessages("test-job-queue", 1),
                    properties, INGEST_JOB_QUEUE_URL, statusStore, tableName);
            PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(0, 1);

            // When / Then
            assertThatCode(() -> wait.pollUntilFinished(poll))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldFinishWaitingForQueueEstimateWhenJobHasAlreadyFinished() {
            // Given
            CompactionJob job = jobHelper.singleFileCompaction();
            RecordsProcessedSummary summary = summary(
                    Instant.parse("2023-05-12T12:16:42Z"), Duration.ofSeconds(30), 10L, 10L);
            statusStore.jobCreated(job);
            statusStore.jobStarted(job, summary.getStartTime(), "test-task");
            statusStore.jobFinished(job, summary, "test-task");

            properties.set(INGEST_JOB_QUEUE_URL, "test-job-queue");
            WaitForQueueEstimate wait = WaitForQueueEstimate.containsUnfinishedJobs(
                    singleQueueVisibleMessages("test-job-queue", 0),
                    properties, INGEST_JOB_QUEUE_URL, statusStore, tableName);
            PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(0, 1);

            // When / Then
            assertThatCode(() -> wait.pollUntilFinished(poll))
                    .doesNotThrowAnyException();
        }
    }
}
