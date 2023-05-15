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
import sleeper.job.common.QueueMessageCount;
import sleeper.job.common.QueueMessageCountsSequence;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.core.record.process.RecordsProcessedSummaryTestData.summary;
import static sleeper.job.common.QueueMessageCountsInMemory.visibleMessages;

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

    private WaitForCurrentSplitAddingMissingJobs waitAndAddJobs(
            Runnable invokeCompactionJobLambda, Runnable invokeCompactionTaskLambda, QueueMessageCount.Client queueClient) {
        return WaitForCurrentSplitAddingMissingJobs.builder()
                .instanceProperties(properties)
                .lambdaClient((lambdaFunctionProperty -> {
                    if (lambdaFunctionProperty.equals(COMPACTION_JOB_CREATION_LAMBDA_FUNCTION)) {
                        invokeCompactionJobLambda.run();
                    } else if (lambdaFunctionProperty.equals(SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION)) {
                        invokeCompactionTaskLambda.run();
                    }
                }))
                .store(statusStore)
                .tableName(tableName)
                .waitForSplitsToFinish(PollWithRetries.intervalAndMaxPolls(0, 1))
                .waitForCompactionsToAppearOnQueue(PollWithRetries.intervalAndMaxPolls(0, 1))
                .queueClient(queueClient)
                .build();
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
        QueueMessageCount.Client sequence = QueueMessageCountsSequence.inOrder(
                visibleMessages(COMPACTION_JOB_QUEUE_URL, 1)
        );

        // When/Then
        assertThat(waitAndAddJobs(invokeCompactionJobLambda, invokeCompactionTaskLambda, sequence)
                .checkIfSplittingCompactionNeededAndWait()).isTrue();
    }
}
