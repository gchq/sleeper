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
import com.amazonaws.services.sqs.AmazonSQS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.deploy.InvokeLambda;
import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobSerDe;
import sleeper.ingest.status.store.task.IngestTaskStatusStoreFactory;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.time.Duration;
import java.util.List;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.INGEST_LAMBDA_FUNCTION;

public class IngestByQueueDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestByQueueDriver.class);

    private final SleeperInstanceContext instance;
    private final AmazonDynamoDB dynamoDBClient;
    private final LambdaClient lambdaClient;
    private final AmazonSQS sqsClient;

    public IngestByQueueDriver(SleeperInstanceContext instance, AmazonDynamoDB dynamoDBClient, LambdaClient lambdaClient, AmazonSQS sqsClient) {
        this.instance = instance;
        this.dynamoDBClient = dynamoDBClient;
        this.lambdaClient = lambdaClient;
        this.sqsClient = sqsClient;
    }

    public void sendJob(InstanceProperty queueUrl, String jobId, List<String> files) {
        sqsClient.sendMessage(instance.getInstanceProperties().get(queueUrl),
                new IngestJobSerDe().toJson(IngestJob.builder()
                        .id(jobId)
                        .tableName(instance.getTableName())
                        .files(files)
                        .build()));
    }

    public void invokeStandardIngestTask() throws InterruptedException {
        invokeStandardIngestTasks(1,
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(3)));
    }

    public void invokeStandardIngestTasks(int expectedTasks, PollWithRetries poll) throws InterruptedException {
        IngestTaskStatusStore taskStatusStore = IngestTaskStatusStoreFactory.getStatusStore(dynamoDBClient, instance.getInstanceProperties());
        int tasksFinishedBefore = taskStatusStore.getAllTasks().size() - taskStatusStore.getTasksInProgress().size();
        poll.pollUntil("tasks are started", () -> {
            InvokeLambda.invokeWith(lambdaClient, instance.getInstanceProperties().get(INGEST_LAMBDA_FUNCTION));
            int tasksStarted = taskStatusStore.getAllTasks().size() - tasksFinishedBefore;
            LOGGER.info("Found {} new ingest tasks", tasksStarted);
            return tasksStarted >= expectedTasks;
        });
    }
}
