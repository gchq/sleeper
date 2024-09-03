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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.deploy.InvokeLambda;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.ingest.status.store.task.IngestTaskStatusStoreFactory;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.ingest.InvokeIngestTasks;
import sleeper.systemtest.dsl.ingest.InvokeIngestTasksDriver;
import sleeper.systemtest.dsl.ingest.InvokeIngestTasksDriverNew;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_LAMBDA_FUNCTION;

public class AwsInvokeIngestTasksDriver implements InvokeIngestTasksDriver, InvokeIngestTasksDriverNew {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsInvokeIngestTasksDriver.class);

    private final SystemTestInstanceContext instance;
    private final AmazonDynamoDB dynamoDBClient;
    private final LambdaClient lambdaClient;

    public AwsInvokeIngestTasksDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.dynamoDBClient = clients.getDynamoDB();
        this.lambdaClient = clients.getLambda();
    }

    @Override
    public InvokeIngestTasks invokeTasksForCurrentInstance() {
        IngestJobStatusStore statusStore = IngestJobStatusStoreFactory.getStatusStore(dynamoDBClient, instance.getInstanceProperties());
        return new InvokeIngestTasks(this::invokeTasksForCurrentInstance, statusStore);
    }

    public void invokeStandardIngestTaskCreator() {
        InvokeLambda.invokeWith(lambdaClient, instance.getInstanceProperties().get(INGEST_LAMBDA_FUNCTION));
    }

    public void invokeStandardIngestTasks(int expectedTasks, PollWithRetries poll) {
        InstanceProperties instanceProperties = instance.getInstanceProperties();
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
