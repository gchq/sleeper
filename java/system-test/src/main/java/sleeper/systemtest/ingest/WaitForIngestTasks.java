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
package sleeper.systemtest.ingest;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.ingest.status.store.task.DynamoDBIngestTaskStatusStore;
import sleeper.ingest.task.IngestTaskStatus;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.systemtest.SystemTestProperties;
import sleeper.systemtest.util.PollWithRetries;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;

public class WaitForIngestTasks {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForIngestTasks.class);
    private static final long POLL_INTERVAL_MILLIS = 30000;
    private static final int MAX_POLLS = 30;

    private final SystemTestProperties systemTestProperties;
    private final AmazonSQS sqsClient;
    private final IngestTaskStatusStore statusStore;
    private final PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(POLL_INTERVAL_MILLIS, MAX_POLLS);

    public WaitForIngestTasks(
            SystemTestProperties systemTestProperties,
            AmazonSQS sqsClient,
            IngestTaskStatusStore statusStore) {
        this.systemTestProperties = systemTestProperties;
        this.sqsClient = sqsClient;
        this.statusStore = statusStore;
    }

    public void pollUntilFinished() throws InterruptedException {
        poll.pollUntil("ingest queue is empty", this::isIngestQueueEmpty);
        poll.pollUntil("ingest tasks finished", this::isIngestTasksFinished);
    }

    private boolean isIngestQueueEmpty() {
        GetQueueAttributesResult result = sqsClient.getQueueAttributes(new GetQueueAttributesRequest()
                .withQueueUrl(systemTestProperties.get(INGEST_JOB_QUEUE_URL))
                .withAttributeNames(QueueAttributeName.ApproximateNumberOfMessages));
        int numberOfMessages = Integer.parseInt(result.getAttributes()
                .get(QueueAttributeName.ApproximateNumberOfMessages.toString()));
        LOGGER.info("Jobs on ingest queue: {}", numberOfMessages);
        return numberOfMessages == 0;
    }

    private boolean isIngestTasksFinished() {
        List<IngestTaskStatus> tasks = statusStore.getTasksInProgress();
        LOGGER.info("Ingest task statuses: {}", tasks.stream().map(IngestTaskStatusJson::new).collect(Collectors.toList()));
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
        IngestTaskStatusStore ingestTaskStatusStore = DynamoDBIngestTaskStatusStore.from(dynamoDBClient, systemTestProperties);

        WaitForIngestTasks wait = new WaitForIngestTasks(systemTestProperties, sqsClient, ingestTaskStatusStore);
        wait.pollUntilFinished();
        s3Client.shutdown();
        dynamoDBClient.shutdown();
    }
}
