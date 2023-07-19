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
package sleeper.systemtest.drivers.ingest;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.status.store.task.IngestTaskStatusStoreFactory;
import sleeper.ingest.task.IngestTaskStatus;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.systemtest.configuration.SystemTestProperties;
import sleeper.systemtest.drivers.util.WaitForQueueEstimate;

import java.io.IOException;
import java.util.List;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.job.common.QueueMessageCount.withSqsClient;

public class WaitForIngestTasks {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForIngestTasks.class);
    private static final long TASKS_FINISHED_POLL_INTERVAL_MILLIS = 30000;
    private static final int TASKS_FINISHED_TIMEOUT_MILLIS = 15 * 60 * 1000;

    // There's a common case where Fargate doesn't have capacity to create a task for every ingest job.
    // In that case we need to wait for the running tasks to finish their jobs and pick up another one.
    // To accommodate this, the poll rate is set low and the timeout is set high.
    private static final long QUEUE_EMPTY_POLL_INTERVAL_MILLIS = 60000;
    private static final int QUEUE_EMPTY_TIMEOUT_MILLIS = 60 * 60 * 1000;

    private final IngestTaskStatusStore taskStatusStore;
    private final WaitForQueueEstimate waitForEmptyQueue;
    private final PollWithRetries poll = PollWithRetries.intervalAndPollingTimeout(
            TASKS_FINISHED_POLL_INTERVAL_MILLIS, TASKS_FINISHED_TIMEOUT_MILLIS);

    public WaitForIngestTasks(
            InstanceProperties instanceProperties,
            AmazonSQS sqsClient,
            IngestTaskStatusStore taskStatusStore) {
        this.taskStatusStore = taskStatusStore;
        this.waitForEmptyQueue = WaitForQueueEstimate.isEmpty(
                withSqsClient(sqsClient), instanceProperties, INGEST_JOB_QUEUE_URL,
                PollWithRetries.intervalAndPollingTimeout(QUEUE_EMPTY_POLL_INTERVAL_MILLIS, QUEUE_EMPTY_TIMEOUT_MILLIS));
    }

    public void pollUntilFinished() throws InterruptedException {
        waitForEmptyQueue.pollUntilFinished();
        poll.pollUntil("ingest tasks finished", this::isIngestTasksFinished);
    }

    private boolean isIngestTasksFinished() {
        List<IngestTaskStatus> tasks = taskStatusStore.getTasksInProgress();
        LOGGER.info("{} ingest tasks still running", tasks.size());
        return tasks.isEmpty();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 1) {
            System.out.println("Usage: <instance id>");
            return;
        }

        String instanceId = args[0];

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();

        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.loadFromS3GivenInstanceId(s3Client, instanceId);
        IngestTaskStatusStore taskStatusStore = IngestTaskStatusStoreFactory.getStatusStore(dynamoDBClient, systemTestProperties);

        WaitForIngestTasks wait = new WaitForIngestTasks(systemTestProperties, sqsClient, taskStatusStore);
        wait.pollUntilFinished();
        s3Client.shutdown();
        dynamoDBClient.shutdown();
    }
}
