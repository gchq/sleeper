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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.autoscaling.AutoScalingClient;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.deploy.InvokeLambda;
import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.core.job.CompactionJobStatusStore;
import sleeper.compaction.core.job.creation.CreateCompactionJobs;
import sleeper.compaction.core.task.CompactionTaskStatus;
import sleeper.compaction.core.task.CompactionTaskStatusStore;
import sleeper.compaction.job.creation.AwsCreateCompactionJobs;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.compaction.CompactionDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.task.common.EC2Scaler;
import sleeper.task.common.RunCompactionTasks;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_TRIGGER_LAMBDA_FUNCTION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;

public class AwsCompactionDriver implements CompactionDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsCompactionDriver.class);

    private final SystemTestInstanceContext instance;
    private final LambdaClient lambdaClient;
    private final AmazonDynamoDB dynamoDBClient;
    private final AmazonS3 s3Client;
    private final AmazonSQS sqsClient;
    private final EcsClient ecsClient;
    private final AutoScalingClient asClient;
    private final Ec2Client ec2Client;
    private final CompactionJobSerDe serDe = new CompactionJobSerDe();

    public AwsCompactionDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.lambdaClient = clients.getLambda();
        this.dynamoDBClient = clients.getDynamoDB();
        this.s3Client = clients.getS3();
        this.sqsClient = clients.getSqs();
        this.ecsClient = clients.getEcs();
        this.asClient = clients.getAutoScaling();
        this.ec2Client = clients.getEc2();
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
        instance.streamTableProperties().parallel().forEach(table -> {
            try {
                CreateCompactionJobs createJobs = AwsCreateCompactionJobs.from(
                        ObjectFactory.noUserJars(), instance.getInstanceProperties(),
                        new StateStoreProvider(instance.getInstanceProperties(), instance::getStateStore),
                        getJobStatusStore(), s3Client, sqsClient);
                createJobs.createJobWithForceAllFiles(table);
            } catch (IOException | ObjectFactoryException e) {
                throw new RuntimeException("Failed creating compaction jobs for table " + table.getStatus(), e);
            }
        });
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
        new RunCompactionTasks(instance.getInstanceProperties(), ecsClient, asClient, ec2Client)
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
        EC2Scaler.create(instance.getInstanceProperties(), asClient, ec2Client).scaleTo(0);
    }

    @Override
    public List<CompactionJob> drainJobsQueueForWholeInstance() {
        InstanceProperties instanceProperties = instance.getInstanceProperties();
        LOGGER.info("Draining compaction jobs queue");
        List<CompactionJob> jobs = Stream.iterate(
                receiveJobs(instanceProperties), not(List::isEmpty), lastJobs -> receiveJobs(instanceProperties))
                .flatMap(List::stream).toList();
        LOGGER.info("Found {} compaction jobs", jobs.size());
        return jobs;
    }

    private List<CompactionJob> receiveJobs(InstanceProperties instanceProperties) {
        String queueUrl = instanceProperties.get(COMPACTION_JOB_QUEUE_URL);
        ReceiveMessageResult receiveResult = sqsClient.receiveMessage(new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withMaxNumberOfMessages(10)
                .withWaitTimeSeconds(5));
        List<Message> messages = receiveResult.getMessages();
        if (messages.isEmpty()) {
            return List.of();
        }
        DeleteMessageBatchResult deleteResult = sqsClient.deleteMessageBatch(new DeleteMessageBatchRequest()
                .withQueueUrl(queueUrl)
                .withEntries(messages.stream()
                        .map(message -> new DeleteMessageBatchRequestEntry()
                                .withId(message.getMessageId())
                                .withReceiptHandle(message.getReceiptHandle()))
                        .toList()));
        if (!deleteResult.getFailed().isEmpty()) {
            throw new RuntimeException("Failed deleting compaction job messages: " + deleteResult.getFailed());
        }
        return messages.stream()
                .map(Message::getBody)
                .map(serDe::fromJson)
                .toList();
    }
}
