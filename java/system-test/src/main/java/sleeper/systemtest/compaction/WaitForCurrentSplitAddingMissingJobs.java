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
import sleeper.systemtest.SystemTestProperties;
import sleeper.systemtest.util.InvokeSystemTestLambda;
import sleeper.systemtest.util.WaitForQueueEstimate;

import java.io.IOException;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;

public class WaitForCurrentSplitAddingMissingJobs {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForCurrentSplitAddingMissingJobs.class);
    private static final long JOBS_ESTIMATE_POLL_INTERVAL_MILLIS = 5000;
    private static final int JOBS_ESTIMATE_MAX_POLLS = 12;

    private final String instanceId;
    private final String tableName;
    private final CompactionJobStatusStore store;
    private final WaitForPartitionSplittingQueue waitForSplitting;
    private final WaitForCompactionJobs waitForCompaction;
    private final WaitForQueueEstimate waitForJobQueueEstimate;

    public WaitForCurrentSplitAddingMissingJobs(
            AmazonSQS sqsClient, CompactionJobStatusStore store,
            InstanceProperties instanceProperties, String tableName) {
        this.instanceId = instanceProperties.get(ID);
        this.tableName = tableName;
        this.store = store;
        waitForSplitting = new WaitForPartitionSplittingQueue(sqsClient, instanceProperties);
        waitForCompaction = new WaitForCompactionJobs(store, tableName);
        waitForJobQueueEstimate = WaitForQueueEstimate.notEmpty(
                sqsClient, instanceProperties, SPLITTING_COMPACTION_JOB_QUEUE_URL,
                PollWithRetries.intervalAndMaxPolls(JOBS_ESTIMATE_POLL_INTERVAL_MILLIS, JOBS_ESTIMATE_MAX_POLLS));
    }

    public void waitForSplittingAndCompaction() throws InterruptedException, IOException {
        LOGGER.info("Waiting for partition splits");
        waitForSplitting.pollUntilFinished();
        checkIfSplittingCompactionNeededAndWait();
    }

    /**
     * @return true if any splitting was done, false if none was needed
     */
    public boolean checkIfSplittingCompactionNeededAndWait() throws InterruptedException, IOException {
        LOGGER.info("Creating compaction jobs");
        InvokeSystemTestLambda.forInstance(instanceId, COMPACTION_JOB_CREATION_LAMBDA_FUNCTION);
        if (store.getUnfinishedJobs(tableName).isEmpty()) {
            LOGGER.info("Lambda created no more jobs, splitting complete");
            return false;
        }
        // SQS message count doesn't always seem to update before task creation Lambda runs, so wait for it
        // (the Lambda decides how many tasks to run based on how many messages it can see are in the queue)
        waitForJobQueueEstimate.pollUntilFinished();
        LOGGER.info("Lambda created new jobs, creating splitting compaction tasks");
        InvokeSystemTestLambda.forInstance(instanceId, SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION);
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

        new WaitForCurrentSplitAddingMissingJobs(sqsClient, store, systemTestProperties, tableName)
                .waitForSplittingAndCompaction();
    }
}
