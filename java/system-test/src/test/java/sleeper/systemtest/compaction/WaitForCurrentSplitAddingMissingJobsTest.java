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
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.testutils.CompactionJobStatusStoreInMemory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.job.common.QueueMessageCount;
import sleeper.systemtest.util.InvokeSystemTestLambda;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static sleeper.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.CompactionJobStatusTestData.startedCompactionRun;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.PARTITION_SPLITTING_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.core.record.process.RecordsProcessedSummaryTestData.summary;
import static sleeper.job.common.QueueMessageCount.approximateNumberVisibleAndNotVisible;
import static sleeper.job.common.QueueMessageCountsInMemory.visibleMessages;
import static sleeper.job.common.QueueMessageCountsSequence.inOrder;

class WaitForCurrentSplitAddingMissingJobsTest {
    private static final String SPLITTING_QUEUE_URL = "test-splitting-queue";
    private static final String COMPACTION_JOB_QUEUE_URL = "test-splitting-compaction-job-queue";
    private final InstanceProperties properties = createTestInstanceProperties();
    private final String tableName = "test-table";
    private final String taskId = "test-task";
    private final CompactionJobTestDataHelper jobHelper = CompactionJobTestDataHelper.forTable(tableName);
    private final CompactionJobStatusStoreInMemory statusStore = new CompactionJobStatusStoreInMemory();
    private final CompactionJob job = jobHelper.singleFileCompaction();
    private final Instant createTime = Instant.parse("2023-05-15T12:11:00Z");
    private final Instant startTime = Instant.parse("2023-05-15T12:12:00Z");
    private final RecordsProcessedSummary summary = summary(
            startTime, Duration.ofMinutes(1), 100L, 100L);

    @BeforeEach
    void setUp() {
        properties.set(PARTITION_SPLITTING_QUEUE_URL, SPLITTING_QUEUE_URL);
        properties.set(SPLITTING_COMPACTION_JOB_QUEUE_URL, COMPACTION_JOB_QUEUE_URL);
    }

    @Test
    void shouldRunOneRoundOfSplitsWithOneSplittingCompactionJob() throws Exception {
        // Given
        WaitForCurrentSplitAddingMissingJobs waiter = runningOneJob()
                .queueClient(inOrder(visibleMessages(COMPACTION_JOB_QUEUE_URL, 1)))
                .waitForCompactionsToAppearOnQueue(pollTimes(1))
                .build();

        // When/Then
        assertThat(waiter.checkIfSplittingCompactionNeededAndWait()).isTrue();
        assertThat(statusStore.getAllJobs(tableName)).containsExactly(
                jobCreated(job, createTime, finishedCompactionRun(taskId, summary)));
    }

    @Test
    void shouldWaitForCompactionJobToAppearOnQueue() throws Exception {
        // Given
        WaitForCurrentSplitAddingMissingJobs waiter = runningOneJob()
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
        WaitForCurrentSplitAddingMissingJobs waiter = runningOneJob()
                .queueClient(inOrder(
                        visibleMessages(COMPACTION_JOB_QUEUE_URL, 0),
                        visibleMessages(COMPACTION_JOB_QUEUE_URL, 0)))
                .waitForCompactionsToAppearOnQueue(pollTimes(2))
                .build();

        // When/Then
        assertThatThrownBy(waiter::checkIfSplittingCompactionNeededAndWait)
                .isInstanceOf(PollWithRetries.TimedOutException.class);
    }

    @Test
    void shouldTimeOutIfCompactionJobDoesNotFinish() {
        // Given
        WaitForCurrentSplitAddingMissingJobs waiter = builderWithDefaults()
                .lambdaClient(invokeCompactionJobAndTaskClient(
                        lambdaWhichCreatesCompactionJob(),
                        lambdaWhichStartsCompactionJob()))
                .queueClient(inOrder(visibleMessages(COMPACTION_JOB_QUEUE_URL, 1)))
                .waitForCompactionsToAppearOnQueue(pollTimes(1))
                .waitForCompactionJobs(pollTimes(2))
                .build();

        // When/Then
        assertThatThrownBy(waiter::checkIfSplittingCompactionNeededAndWait)
                .isInstanceOf(PollWithRetries.TimedOutException.class);
        assertThat(statusStore.getAllJobs(tableName)).containsExactly(
                jobCreated(job, createTime, startedCompactionRun(taskId, startTime)));
    }

    @Test
    void shouldCompleteIfCompactionJobIsFinishedBeforeWeCheckQueueEstimate() throws Exception {
        // Given
        WaitForCurrentSplitAddingMissingJobs waiter = builderWithDefaults()
                .lambdaClient(invokeCompactionJobAndTaskClient(
                        lambdaWhichCreatesAndFinishesCompactionJob(),
                        lambdaWhichDoesNothing()))
                .queueClient(inOrder(visibleMessages(COMPACTION_JOB_QUEUE_URL, 0)))
                .waitForCompactionsToAppearOnQueue(pollTimes(1))
                .waitForCompactionJobs(pollTimes(1))
                .build();

        // When / Then
        assertThat(waiter.checkIfSplittingCompactionNeededAndWait()).isTrue();
        assertThat(statusStore.getAllJobs(tableName)).containsExactly(
                jobCreated(job, createTime, finishedCompactionRun(taskId, summary)));
    }

    @Test
    void shouldCompleteIfCompactionJobIsStartedBeforeWeCheckQueueEstimate() throws Exception {
        // Given
        WaitForCurrentSplitAddingMissingJobs waiter = builderWithDefaults()
                .lambdaClient(invokeCompactionJobAndTaskClient(
                        lambdaWhichCreatesAndStartsCompactionJob(),
                        lambdaWhichDoesNothing()))
                .queueClient(emptyCompactionQueueWhichFinishesJobWhenEstimateIsChecked())
                .waitForCompactionsToAppearOnQueue(pollTimes(1))
                .waitForCompactionJobs(pollTimes(1))
                .build();

        // When / Then
        assertThat(waiter.checkIfSplittingCompactionNeededAndWait()).isTrue();
        assertThat(statusStore.getAllJobs(tableName)).containsExactly(
                jobCreated(job, createTime, finishedCompactionRun(taskId, summary)));
    }

    @Test
    void shouldNotInvokeCompactionTaskCreationIfJobFinishesFirst() throws Exception {
        // Given
        Runnable invokeCompactionTaskLambda = mock(Runnable.class);

        WaitForCurrentSplitAddingMissingJobs waiter = builderWithDefaults()
                .lambdaClient(invokeCompactionJobAndTaskClient(
                        lambdaWhichCreatesAndStartsCompactionJob(),
                        invokeCompactionTaskLambda))
                .queueClient(emptyCompactionQueueWhichFinishesJobWhenEstimateIsChecked())
                .waitForCompactionsToAppearOnQueue(pollTimes(1))
                .waitForCompactionJobs(pollTimes(1))
                .build();

        // When / Then
        waiter.checkIfSplittingCompactionNeededAndWait();
        verifyNoInteractions(invokeCompactionTaskLambda);
    }

    private WaitForCurrentSplitAddingMissingJobs.Builder runningOneJob() {
        Runnable invokeCompactionJobLambda = () -> statusStore.jobCreated(job, createTime);
        Runnable invokeCompactionTaskLambda = () -> {
            statusStore.jobStarted(job, summary.getStartTime(), taskId);
            statusStore.jobFinished(job, summary, taskId);
        };
        return builderWithDefaults()
                .lambdaClient(invokeCompactionJobAndTaskClient(invokeCompactionJobLambda, invokeCompactionTaskLambda));
    }

    private Runnable lambdaWhichCreatesCompactionJob() {
        return () -> statusStore.jobCreated(job, createTime);
    }

    private Runnable lambdaWhichStartsCompactionJob() {
        return () -> statusStore.jobStarted(job, summary.getStartTime(), taskId);
    }

    private Runnable lambdaWhichCreatesAndStartsCompactionJob() {
        return () -> {
            statusStore.jobCreated(job, createTime);
            statusStore.jobStarted(job, summary.getStartTime(), taskId);
        };
    }

    private Runnable lambdaWhichCreatesAndFinishesCompactionJob() {
        return () -> {
            statusStore.jobCreated(job, createTime);
            statusStore.jobStarted(job, summary.getStartTime(), taskId);
            statusStore.jobFinished(job, summary, taskId);
        };
    }

    private static Runnable lambdaWhichDoesNothing() {
        return () -> {
        };
    }

    private QueueMessageCount.Client emptyCompactionQueueWhichFinishesJobWhenEstimateIsChecked() {
        QueueMessageCount.Client queueClient = mock(QueueMessageCount.Client.class);
        when(queueClient.getQueueMessageCount(COMPACTION_JOB_QUEUE_URL))
                .thenAnswer(invocation -> {
                    statusStore.jobFinished(job, summary, taskId);
                    return approximateNumberVisibleAndNotVisible(0, 0);
                });
        return queueClient;
    }

    private WaitForCurrentSplitAddingMissingJobs.Builder builderWithDefaults() {
        return WaitForCurrentSplitAddingMissingJobs.builder()
                .instanceProperties(properties)
                .store(statusStore)
                .tableName(tableName)
                .waitForSplitsToFinish(pollTimes(1))
                .waitForCompactionsToAppearOnQueue(pollTimes(1))
                .waitForCompactionJobs(pollTimes(1));
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
