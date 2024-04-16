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

package sleeper.systemtest.drivers.compaction;

import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.sqs.AmazonSQS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.deploy.InvokeLambda;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.creation.CreateCompactionJobs;
import sleeper.compaction.job.creation.CreateCompactionJobs.Mode;
import sleeper.compaction.job.creation.SendCompactionJobToSqs;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.table.InvokeForTableRequest;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.compaction.CompactionDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.task.common.EC2Scaler;
import sleeper.task.common.RunCompactionTasks;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_TRIGGER_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_BATCH_SIZE;

public class AwsCompactionDriver implements CompactionDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsCompactionDriver.class);

    private final SystemTestInstanceContext instance;
    private final LambdaClient lambdaClient;
    private final AmazonDynamoDB dynamoDBClient;
    private final AmazonSQS sqsClient;
    private final AmazonECS ecsClient;
    private final AmazonAutoScaling asClient;

    public AwsCompactionDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.lambdaClient = clients.getLambda();
        this.dynamoDBClient = clients.getDynamoDB();
        this.sqsClient = clients.getSqs();
        this.ecsClient = clients.getEcs();
        this.asClient = clients.getAutoScaling();
    }

    @Override
    public CompactionJobStatusStore getJobStatusStore() {
        return CompactionJobStatusStoreFactory
                .getStatusStoreWithStronglyConsistentReads(dynamoDBClient, instance.getInstanceProperties());
    }

    @Override
    public void triggerCreateJobs() {
        InvokeLambda.invokeWith(lambdaClient,
                instance.getInstanceProperties().get(COMPACTION_JOB_CREATION_TRIGGER_LAMBDA_FUNCTION));
    }

    @Override
    public void forceCreateJobs() {
        CreateCompactionJobs createJobs = new CreateCompactionJobs(
                ObjectFactory.noUserJars(), instance.getInstanceProperties(),
                instance.getTablePropertiesProvider(), instance.getStateStoreProvider(),
                new SendCompactionJobToSqs(instance.getInstanceProperties(), sqsClient)::send, getJobStatusStore(),
                Mode.FORCE_ALL_FILES_AFTER_STRATEGY);
        int batchSize = instance.getInstanceProperties().getInt(COMPACTION_JOB_CREATION_BATCH_SIZE);
        InvokeForTableRequest.forTables(
                instance.streamTableProperties().map(TableProperties::getStatus),
                batchSize, createJobs::createJobs);
    }

    @Override
    public void invokeTasks(int expectedTasks, PollWithRetries poll) {
        CompactionTaskStatusStore store = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDBClient, instance.getInstanceProperties());
        long tasksFinishedBefore = store.getAllTasks().stream().filter(CompactionTaskStatus::isFinished).count();
        try {
            poll.pollUntil("tasks are started", () -> {
                InvokeLambda.invokeWith(lambdaClient, instance.getInstanceProperties().get(COMPACTION_TASK_CREATION_LAMBDA_FUNCTION));
                long tasksStarted = store.getAllTasks().size() - tasksFinishedBefore;
                LOGGER.info("Found {} running compaction tasks", tasksStarted);
                return tasksStarted >= expectedTasks;
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void forceStartTasks(int numberOfTasks, PollWithRetries poll) {
        CompactionTaskStatusStore store = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDBClient, instance.getInstanceProperties());
        long tasksFinishedBefore = store.getAllTasks().stream().filter(CompactionTaskStatus::isFinished).count();
        new RunCompactionTasks(instance.getInstanceProperties(), ecsClient, asClient)
                .runToMeetTargetTasks(numberOfTasks);
        try {
            poll.pollUntil("tasks are started", () -> {
                long tasksStarted = store.getAllTasks().size() - tasksFinishedBefore;
                LOGGER.info("Found {} running compaction tasks", tasksStarted);
                return tasksStarted >= numberOfTasks;
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void scaleToZero() {
        EC2Scaler.create(instance.getInstanceProperties(), asClient, ecsClient).scaleTo(0);
    }
}
