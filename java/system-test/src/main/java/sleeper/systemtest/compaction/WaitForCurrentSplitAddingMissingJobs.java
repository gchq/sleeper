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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.util.PollWithRetries;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.job.common.QueueMessageCount;
import sleeper.systemtest.SystemTestProperties;
import sleeper.systemtest.util.InvokeSystemTestLambda;
import sleeper.systemtest.util.WaitForQueueEstimate;

import java.io.IOException;
import java.util.Objects;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.PARTITION_SPLITTING_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.job.common.QueueMessageCount.withSqsClient;

public class WaitForCurrentSplitAddingMissingJobs {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForCurrentSplitAddingMissingJobs.class);
    private static final long JOBS_ESTIMATE_POLL_INTERVAL_MILLIS = 5000;
    private static final int JOBS_ESTIMATE_MAX_POLLS = 12;
    private static final long SPLITTING_POLL_INTERVAL_MILLIS = 5000;
    private static final int SPLITTING_MAX_POLLS = 12;
    private static final long COMPACTION_JOB_POLL_INTERVAL_MILLIS = 30000;
    private static final int COMPACTION_JOB_MAX_POLLS = 120;

    private final String tableName;
    private final CompactionJobStatusStore store;
    private final WaitForQueueEstimate waitForSplitsToFinish;
    private final WaitForCompactionJobs waitForCompaction;
    private final WaitForQueueEstimate waitForCompactionsToAppearOnQueue;
    private final InvokeSystemTestLambda.Client lambdaClient;

    private WaitForCurrentSplitAddingMissingJobs(Builder builder) {
        QueueMessageCount.Client queueClient = Objects.requireNonNull(builder.queueClient, "queueClient must not be null");
        InstanceProperties properties = Objects.requireNonNull(builder.instanceProperties, "instanceProperties must not be null");
        tableName = Objects.requireNonNull(builder.tableName, "tableName must not be null");
        store = Objects.requireNonNull(builder.store, "store must not be null");
        lambdaClient = Objects.requireNonNull(builder.lambdaClient, "lambdaClient must not be null");
        waitForSplitsToFinish = WaitForQueueEstimate.isConsumed(
                queueClient, properties, PARTITION_SPLITTING_QUEUE_URL, builder.waitForSplitsToFinish);
        waitForCompaction = new WaitForCompactionJobs(store, tableName, builder.waitForCompactionJobs);
        waitForCompactionsToAppearOnQueue = WaitForQueueEstimate.matchesUnstartedJobs(
                queueClient, properties, SPLITTING_COMPACTION_JOB_QUEUE_URL,
                store, tableName, builder.waitForCompactionsToAppearOnQueue);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static WaitForCurrentSplitAddingMissingJobs from(AmazonSQS sqsClient, CompactionJobStatusStore store,
                                                            InstanceProperties instanceProperties, String tableName) {
        return builder()
                .queueClient(withSqsClient(sqsClient))
                .store(store)
                .instanceProperties(instanceProperties)
                .tableName(tableName)
                .waitForSplitsToFinish(PollWithRetries.intervalAndMaxPolls(
                        SPLITTING_POLL_INTERVAL_MILLIS, SPLITTING_MAX_POLLS))
                .waitForCompactionsToAppearOnQueue(PollWithRetries.intervalAndMaxPolls(
                        JOBS_ESTIMATE_POLL_INTERVAL_MILLIS, JOBS_ESTIMATE_MAX_POLLS))
                .waitForCompactionJobs(PollWithRetries.intervalAndMaxPolls(
                        COMPACTION_JOB_POLL_INTERVAL_MILLIS, COMPACTION_JOB_MAX_POLLS))
                .lambdaClient(InvokeSystemTestLambda.client(instanceProperties))
                .build();
    }

    public void waitForSplittingAndCompaction() throws InterruptedException {
        LOGGER.info("Waiting for partition splits");
        waitForSplitsToFinish.pollUntilFinished();
        checkIfSplittingCompactionNeededAndWait();
    }

    /**
     * @return true if any splitting was done, false if none was needed
     */
    public boolean checkIfSplittingCompactionNeededAndWait() throws InterruptedException {
        LOGGER.info("Creating compaction jobs");
        int numJobsBefore = store.getAllJobs(tableName).size();
        lambdaClient.invokeLambda(COMPACTION_JOB_CREATION_LAMBDA_FUNCTION);
        int numJobsAfter = store.getAllJobs(tableName).size();
        if (numJobsAfter == numJobsBefore) {
            LOGGER.info("Lambda created no more jobs, splitting complete");
            return false;
        }
        // SQS message count doesn't always seem to update before task creation Lambda runs, so wait for it
        // (the Lambda decides how many tasks to run based on how many messages it can see are in the queue)
        waitForCompactionsToAppearOnQueue.pollUntilFinished();
        if (store.getUnstartedJobs(tableName).isEmpty()) {
            LOGGER.info("Lambda created new jobs, but they were picked up by another running task");
        } else {
            LOGGER.info("Lambda created new jobs, creating splitting compaction tasks");
            lambdaClient.invokeLambda(SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION);
        }
        waitForCompaction.pollUntilFinished();
        return true;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 2) {
            System.out.println("Usage: <instance id> <table name>");
            return;
        }

        String instanceId = args[0];
        String tableName = args[1];

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();

        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.loadFromS3GivenInstanceId(s3Client, instanceId);
        CompactionJobStatusStore store = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, systemTestProperties);

        WaitForCurrentSplitAddingMissingJobs.from(sqsClient, store, systemTestProperties, tableName)
                .waitForSplittingAndCompaction();
    }

    public static final class Builder {
        private InstanceProperties instanceProperties;
        private String tableName;
        private CompactionJobStatusStore store;
        private QueueMessageCount.Client queueClient;
        private PollWithRetries waitForSplitsToFinish;
        private PollWithRetries waitForCompactionsToAppearOnQueue;
        private PollWithRetries waitForCompactionJobs;
        private InvokeSystemTestLambda.Client lambdaClient;

        private Builder() {
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder store(CompactionJobStatusStore store) {
            this.store = store;
            return this;
        }

        public Builder queueClient(QueueMessageCount.Client queueClient) {
            this.queueClient = queueClient;
            return this;
        }

        public Builder waitForSplitsToFinish(PollWithRetries waitForSplitsToFinish) {
            this.waitForSplitsToFinish = waitForSplitsToFinish;
            return this;
        }

        public Builder waitForCompactionsToAppearOnQueue(PollWithRetries waitForCompactionsToAppearOnQueue) {
            this.waitForCompactionsToAppearOnQueue = waitForCompactionsToAppearOnQueue;
            return this;
        }

        public Builder waitForCompactionJobs(PollWithRetries waitForCompactionJobs) {
            this.waitForCompactionJobs = waitForCompactionJobs;
            return this;
        }

        public Builder lambdaClient(InvokeSystemTestLambda.Client lambdaClient) {
            this.lambdaClient = lambdaClient;
            return this;
        }

        public WaitForCurrentSplitAddingMissingJobs build() {
            return new WaitForCurrentSplitAddingMissingJobs(this);
        }
    }
}
