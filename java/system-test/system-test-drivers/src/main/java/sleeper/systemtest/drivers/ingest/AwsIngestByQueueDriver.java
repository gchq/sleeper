/*
 * Copyright 2022-2024 Crown Copyright
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
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobSerDe;
import sleeper.ingest.status.store.task.IngestTaskStatusStoreFactory;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.ingest.IngestByQueueDriver;

import java.util.List;
import java.util.UUID;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_LAMBDA_FUNCTION;

public class AwsIngestByQueueDriver implements IngestByQueueDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsIngestByQueueDriver.class);

    private final AmazonDynamoDB dynamoDBClient;
    private final LambdaClient lambdaClient;
    private final AmazonSQS sqsClient;

    public AwsIngestByQueueDriver(SystemTestClients clients) {
        this.dynamoDBClient = clients.getDynamoDB();
        this.lambdaClient = clients.getLambda();
        this.sqsClient = clients.getSqs();
    }

    public String sendJobGetId(String queueUrl, String tableName, List<String> files) {
        String jobId = UUID.randomUUID().toString();
        LOGGER.info("Sending ingest job {} with {} files to queue: {}", jobId, files.size(), queueUrl);
        sqsClient.sendMessage(queueUrl,
                new IngestJobSerDe().toJson(IngestJob.builder()
                        .id(jobId)
                        .tableName(tableName)
                        .files(files)
                        .build()));
        return jobId;
    }

    public void invokeStandardIngestTasks(InstanceProperties instanceProperties, int expectedTasks, PollWithRetries poll) {
        IngestTaskStatusStore taskStatusStore = IngestTaskStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);
        int tasksFinishedBefore = taskStatusStore.getAllTasks().size() - taskStatusStore.getTasksInProgress().size();
        try {
            poll.pollUntil("tasks are started", () -> {
                InvokeLambda.invokeWith(lambdaClient, instanceProperties.get(INGEST_LAMBDA_FUNCTION));
                int tasksStarted = taskStatusStore.getAllTasks().size() - tasksFinishedBefore;
                LOGGER.info("Found {} new ingest tasks", tasksStarted);
                return tasksStarted >= expectedTasks;
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
