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

package sleeper.systemtest.compaction;

import org.junit.jupiter.api.Test;

import sleeper.clients.util.PollWithRetries;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.testutils.CompactionJobStatusStoreInMemory;
import sleeper.configuration.properties.InstanceProperties;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;
import static sleeper.job.common.QueueMessageCountsInMemory.singleQueueVisibleMessages;

public class WaitForSplittingJobsToBeConsumedTest {
    InstanceProperties properties = createTestInstanceProperties();
    String tableName = "test-table";
    CompactionJobTestDataHelper jobHelper = CompactionJobTestDataHelper.forTable(tableName);
    CompactionJobStatusStore statusStore = new CompactionJobStatusStoreInMemory();

    @Test
    void shouldFinishWhenQueueIsNotEmptyAndNoJobsArePending() {
        // Given
        CompactionJob job = jobHelper.singleFileCompaction();
        statusStore.jobCreated(job);
        statusStore.jobStarted(job, Instant.parse("2023-05-12T14:32:00Z"), "test-task");
        properties.set(SPLITTING_COMPACTION_JOB_QUEUE_URL, "test-job-queue");
        WaitForSplittingJobsToBeConsumed waitForJobs = new WaitForSplittingJobsToBeConsumed(
                singleQueueVisibleMessages("test-job-queue", 1),
                properties, tableName, statusStore);
        PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(0, 1);

        // When/Then
        assertThatCode(() -> waitForJobs.pollUntilFinished(poll))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldFinishWhenQueueEmptyAndJobsArePending() {
        // Given
        statusStore.jobCreated(jobHelper.singleFileCompaction());

        properties.set(SPLITTING_COMPACTION_JOB_QUEUE_URL, "test-job-queue");
        WaitForSplittingJobsToBeConsumed waitForJobs = new WaitForSplittingJobsToBeConsumed(
                singleQueueVisibleMessages("test-job-queue", 0),
                properties, tableName, statusStore);
        PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(0, 1);

        // When/Then
        assertThatCode(() -> waitForJobs.pollUntilFinished(poll))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldTimeoutWhenQueueEmptyAndStatusStoreIsEmpty() {
        // Given
        properties.set(SPLITTING_COMPACTION_JOB_QUEUE_URL, "test-job-queue");
        WaitForSplittingJobsToBeConsumed waitForJobs = new WaitForSplittingJobsToBeConsumed(
                singleQueueVisibleMessages("test-job-queue", 0),
                properties, tableName, statusStore);
        PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(0, 1);

        // When/Then
        assertThatThrownBy(() -> waitForJobs.pollUntilFinished(poll))
                .isInstanceOf(PollWithRetries.TimedOutException.class);
    }

    @Test
    void shouldTimeoutWhenQueueEmptyAndNoJobsArePending() {
        // Given
        CompactionJob job = jobHelper.singleFileCompaction();
        statusStore.jobCreated(job);
        statusStore.jobStarted(job, Instant.parse("2023-05-12T14:32:00Z"), "test-task");

        properties.set(SPLITTING_COMPACTION_JOB_QUEUE_URL, "test-job-queue");
        WaitForSplittingJobsToBeConsumed waitForJobs = new WaitForSplittingJobsToBeConsumed(
                singleQueueVisibleMessages("test-job-queue", 0),
                properties, tableName, statusStore);
        PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(0, 1);

        // When/Then
        assertThatThrownBy(() -> waitForJobs.pollUntilFinished(poll))
                .isInstanceOf(PollWithRetries.TimedOutException.class);
    }
}
