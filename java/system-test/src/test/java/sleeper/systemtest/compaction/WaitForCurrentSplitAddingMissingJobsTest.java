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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.clients.util.PollWithRetries;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.testutils.CompactionJobStatusStoreInMemory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.systemtest.util.InvokeSystemTestLambda;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.core.record.process.RecordsProcessedSummaryTestData.summary;
import static sleeper.job.common.QueueMessageCountsInMemory.visibleMessages;
import static sleeper.job.common.QueueMessageCountsSequence.inOrder;

class WaitForCurrentSplitAddingMissingJobsTest {
    private static final String COMPACTION_JOB_QUEUE_URL = "test-splitting-compaction-job-queue";
    private final InstanceProperties properties = createTestInstanceProperties();
    private final String tableName = "test-table";
    private final CompactionJobTestDataHelper jobHelper = CompactionJobTestDataHelper.forTable(tableName);
    private final CompactionJobStatusStore statusStore = new CompactionJobStatusStoreInMemory();

    @BeforeEach
    void setUp() {
        properties.set(SPLITTING_COMPACTION_JOB_QUEUE_URL, COMPACTION_JOB_QUEUE_URL);
    }

    @Test
    void shouldRunOneRoundOfSplitsWithOneSplittingCompactionJob() throws Exception {
        // Given
        CompactionJob job = jobHelper.singleFileCompaction();
        Instant startTime = Instant.parse("2023-05-15T12:12:00Z");
        Runnable invokeCompactionJobLambda = () -> statusStore.jobCreated(job);
        Runnable invokeCompactionTaskLambda = () -> {
            statusStore.jobStarted(job, startTime, "test-task");
            statusStore.jobFinished(job, summary(startTime, Duration.ofMinutes(1), 100L, 100L), "test-task");
        };
        WaitForCurrentSplitAddingMissingJobs waiter = builder()
                .lambdaClient(invokeCompactionJobAndTaskClient(invokeCompactionJobLambda, invokeCompactionTaskLambda))
                .queueClient(inOrder(visibleMessages(COMPACTION_JOB_QUEUE_URL, 1)))
                .waitForCompactionsToAppearOnQueue(pollTimes(1))
                .build();

        // When/Then
        assertThat(waiter.checkIfSplittingCompactionNeededAndWait()).isTrue();
    }

    @Test
    void shouldWaitForCompactionJobToAppearOnQueue() throws Exception {
        // Given
        CompactionJob job = jobHelper.singleFileCompaction();
        Instant startTime = Instant.parse("2023-05-15T12:12:00Z");
        Runnable invokeCompactionJobLambda = () -> statusStore.jobCreated(job);
        Runnable invokeCompactionTaskLambda = () -> {
            statusStore.jobStarted(job, startTime, "test-task");
            statusStore.jobFinished(job, summary(startTime, Duration.ofMinutes(1), 100L, 100L), "test-task");
        };
        WaitForCurrentSplitAddingMissingJobs waiter = builder()
                .lambdaClient(invokeCompactionJobAndTaskClient(invokeCompactionJobLambda, invokeCompactionTaskLambda))
                .queueClient(inOrder(
                        visibleMessages(COMPACTION_JOB_QUEUE_URL, 0),
                        visibleMessages(COMPACTION_JOB_QUEUE_URL, 1)))
                .waitForCompactionsToAppearOnQueue(pollTimes(2))
                .build();

        // When/Then
        assertThat(waiter.checkIfSplittingCompactionNeededAndWait()).isTrue();
    }

    @Test
    void shouldTimeOutIfCompactionJobDoesNotAppearOnQueue() {
        // Given
        CompactionJob job = jobHelper.singleFileCompaction();
        Instant startTime = Instant.parse("2023-05-15T12:12:00Z");
        Runnable invokeCompactionJobLambda = () -> statusStore.jobCreated(job);
        Runnable invokeCompactionTaskLambda = () -> {
            statusStore.jobStarted(job, startTime, "test-task");
            statusStore.jobFinished(job, summary(startTime, Duration.ofMinutes(1), 100L, 100L), "test-task");
        };
        WaitForCurrentSplitAddingMissingJobs waiter = builder()
                .lambdaClient(invokeCompactionJobAndTaskClient(invokeCompactionJobLambda, invokeCompactionTaskLambda))
                .queueClient(inOrder(
                        visibleMessages(COMPACTION_JOB_QUEUE_URL, 0),
                        visibleMessages(COMPACTION_JOB_QUEUE_URL, 0)))
                .waitForCompactionsToAppearOnQueue(pollTimes(2))
                .build();

        // When/Then
        assertThatThrownBy(waiter::checkIfSplittingCompactionNeededAndWait)
                .isInstanceOf(PollWithRetries.TimedOutException.class);
    }

    private WaitForCurrentSplitAddingMissingJobs.Builder builder() {
        return WaitForCurrentSplitAddingMissingJobs.builder()
                .instanceProperties(properties)
                .store(statusStore)
                .tableName(tableName)
                .waitForSplitsToFinish(pollTimes(0))
                .waitForCompactionsToAppearOnQueue(pollTimes(0));
    }

    private static InvokeSystemTestLambda.Client invokeCompactionJobAndTaskClient(
            Runnable invokeCompactionJobLambda, Runnable invokeCompactionTaskLambda) {
        return lambdaFunctionProperty -> {
            if (lambdaFunctionProperty.equals(COMPACTION_JOB_CREATION_LAMBDA_FUNCTION)) {
                invokeCompactionJobLambda.run();
            } else if (lambdaFunctionProperty.equals(SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION)) {
                invokeCompactionTaskLambda.run();
            }
        };
    }

    private static PollWithRetries pollTimes(int polls) {
        return PollWithRetries.intervalAndMaxPolls(0, polls);
    }
}
