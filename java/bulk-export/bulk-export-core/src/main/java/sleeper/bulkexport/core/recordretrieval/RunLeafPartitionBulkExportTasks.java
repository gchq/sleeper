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
package sleeper.bulkexport.core.recordretrieval;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import software.amazon.awssdk.services.ecs.EcsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.autoscaling.AutoScalingClient;
import software.amazon.awssdk.services.ecs.model.AwsVpcConfiguration;
import software.amazon.awssdk.services.ecs.model.ContainerOverride;
import software.amazon.awssdk.services.ecs.model.LaunchType;
import software.amazon.awssdk.services.ecs.model.NetworkConfiguration;
import software.amazon.awssdk.services.ecs.model.PropagateTags;
import software.amazon.awssdk.services.ecs.model.RunTaskRequest;
import software.amazon.awssdk.services.ecs.model.TaskOverride;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.task.common.ECSTaskCount;
import sleeper.task.common.QueueMessageCount;
import sleeper.task.common.RunECSTasks;

import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;

import static sleeper.core.ContainerConstants.COMPACTION_CONTAINER_NAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_FARGATE_DEFINITION_FAMILY;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_BULK_EXPORT_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.ECS_SECURITY_GROUPS;
import static sleeper.core.properties.instance.CommonProperty.FARGATE_VERSION;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CompactionProperty.MAXIMUM_CONCURRENT_COMPACTION_TASKS;

/**
 * Finds the number of messages on a queue, and starts up a Fargate
 * task for each, up to a configurable maximum.
 */
public class RunLeafPartitionBulkExportTasks {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunLeafPartitionBulkExportTasks.class);

    private final InstanceProperties instanceProperties;
    private final TaskCounts taskCounts;
    private final TaskLauncher taskLauncher;

    public RunLeafPartitionBulkExportTasks(
            InstanceProperties instanceProperties, EcsClient ecsClient) {
        this(instanceProperties,
                () -> ECSTaskCount.getNumPendingAndRunningTasks(instanceProperties.get(COMPACTION_CLUSTER), ecsClient),
                (numberOfTasks, checkAbort) -> launchTasks(ecsClient, instanceProperties, numberOfTasks, checkAbort));
    }

    public RunLeafPartitionBulkExportTasks(InstanceProperties instanceProperties, TaskCounts taskCounts, TaskLauncher taskLauncher) {
        this.instanceProperties = instanceProperties;
        this.taskCounts = taskCounts;
        this.taskLauncher = taskLauncher;
    }

    public interface TaskCounts {
        int getRunningAndPending();
    }

    public interface TaskLauncher {
        void launchTasks(int numberOfTasksToCreate, BooleanSupplier checkAbort);
    }

    public void run(QueueMessageCount.Client queueMessageCount) {
        long startTime = System.currentTimeMillis();
        String sqsJobQueueUrl = instanceProperties.get(LEAF_PARTITION_BULK_EXPORT_QUEUE_URL);
        int maximumRunningTasks = instanceProperties.getInt(MAXIMUM_CONCURRENT_COMPACTION_TASKS);
        LOGGER.info("Queue URL is {}", sqsJobQueueUrl);
        // Find out number of messages in queue that are not being processed
        int queueSize = queueMessageCount.getQueueMessageCount(sqsJobQueueUrl)
                .getApproximateNumberOfMessages();
        LOGGER.info("Queue size is {}", queueSize);
        // Request 1 task for each item on the queue
        LOGGER.info("Maximum concurrent tasks is {}", maximumRunningTasks);
        int numRunningAndPendingTasks = taskCounts.getRunningAndPending();
        LOGGER.info("Number of running and pending tasks is {}", numRunningAndPendingTasks);

        int maxTasksToCreate = maximumRunningTasks - numRunningAndPendingTasks;
        int numberOfTasksToCreate = Math.min(queueSize, maxTasksToCreate);
        int targetTasks = numRunningAndPendingTasks + numberOfTasksToCreate;
        scaleToHostsAndLaunchTasks(targetTasks, numberOfTasksToCreate, () -> {
            // This lambda is triggered every minute so abort once get
            // close to 1 minute
            if (System.currentTimeMillis() - startTime > 50 * 1000L) {
                LOGGER.info("Running for more than 50 seconds, aborting");
                return true;
            } else {
                return false;
            }
        });
    }

    /**
     * Ensure that a given number of compaction tasks are running.
     *
     * If there are currently less tasks running, then new ones will be started.
     * The scaler will be invoked to ensure the ECS cluster can accommodate the
     * necessary new tasks
     * (may be a delay in launching instances, so initial task launch may fail).
     *
     * @param desiredTaskCount the number of tasks to ensure are running
     */
    public void runToMeetTargetTasks(int desiredTaskCount) {
        int numRunningAndPendingTasks = taskCounts.getRunningAndPending();
        LOGGER.info("Number of running and pending tasks is {}", numRunningAndPendingTasks);
        int numberOfTasksToCreate = desiredTaskCount - numRunningAndPendingTasks;
        // Instruct the scaler to ensure we have room for the correct number of running
        // tasks. We use max() here
        // because we are either at or need to grow to the desired number. If the
        // desiredTaskCount is LESS than
        // the number of running/pending tasks, we don't want to instruct the scaler to
        // shrink the ECS cluster.
        scaleToHostsAndLaunchTasks(Math.max(desiredTaskCount, numRunningAndPendingTasks), numberOfTasksToCreate,
                () -> false);
    }

    private void scaleToHostsAndLaunchTasks(int targetTasks, int createTasks, BooleanSupplier checkAbort) {
        LOGGER.info("Target number of tasks is {}", targetTasks);
        LOGGER.info("Tasks to create is {}", createTasks);
        if (targetTasks < 0) {
            throw new IllegalArgumentException("targetTasks is < 0");
        }
        //hostScaler.scaleTo(targetTasks);
        if (createTasks < 1) {
            LOGGER.info("Finishing as no new tasks are needed");
            return;
        }
        taskLauncher.launchTasks(createTasks, checkAbort);
    }

    /**
     * Attempts to launch some tasks on ECS.
     *
     * @param ecsClient             Amazon ECS client
     * @param instanceProperties    Properties for instance
     * @param numberOfTasksToCreate number of tasks to create
     * @param checkAbort            a condition under which launching will be
     *                              aborted
     */
    private static void launchTasks(
            EcsClient ecsClient, InstanceProperties instanceProperties,
            int numberOfTasksToCreate, BooleanSupplier checkAbort) {
        RunECSTasks.runTasks(builder -> builder
                .ecsClient(ecsClient)
                .runTaskRequest(createRunTaskRequest(instanceProperties))
                .numberOfTasksToCreate(numberOfTasksToCreate)
                .checkAbort(checkAbort));
    }

    /**
     * Creates a new task request that can be passed to ECS.
     *
     * @param instanceProperties the instance properties
     * @return the request for ECS
     * @throws IllegalArgumentException if <code>launchType</code> is FARGATE and
     *                                  version is null
     */
    private static RunTaskRequest createRunTaskRequest(InstanceProperties instanceProperties) {
        TaskOverride override = createOverride(instanceProperties);
        RunTaskRequest.Builder runTaskRequest = RunTaskRequest.builder()
                .cluster(instanceProperties.get(COMPACTION_CLUSTER))
                .overrides(override)
                .propagateTags(PropagateTags.TASK_DEFINITION);

        String fargateVersion = Objects.requireNonNull(instanceProperties.get(FARGATE_VERSION),
                "fargateVersion cannot be null");
        NetworkConfiguration networkConfiguration = networkConfig(instanceProperties);
        runTaskRequest
                .launchType(LaunchType.FARGATE)
                .platformVersion(fargateVersion)
                .networkConfiguration(networkConfiguration)
                .taskDefinition(instanceProperties.get(COMPACTION_TASK_FARGATE_DEFINITION_FAMILY));

        return runTaskRequest.build();
    }

    /**
     * Create the container definition overrides for the task launch.
     *
     * @param instanceProperties the instance properties
     * @return the container definition overrides
     */
    private static TaskOverride createOverride(InstanceProperties instanceProperties) {
        ContainerOverride containerOverride = ContainerOverride.builder()
                .name(COMPACTION_CONTAINER_NAME)
                .command(List.of(instanceProperties.get(CONFIG_BUCKET)))
                .build();
        return TaskOverride.builder()
                .containerOverrides(containerOverride)
                .build();
    }

    /**
     * Create the container networking configuration.
     *
     * @param instanceProperties the instance properties
     * @return task network configuration
     */
    private static NetworkConfiguration networkConfig(InstanceProperties instanceProperties) {
        AwsVpcConfiguration vpcConfiguration = AwsVpcConfiguration.builder()
                .subnets(instanceProperties.getList(SUBNETS))
                .securityGroups(instanceProperties.getList(ECS_SECURITY_GROUPS))
                .build();
        return NetworkConfiguration.builder()
                .awsvpcConfiguration(vpcConfiguration)
                .build();
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: <instance-id> <number-of-tasks>");
            return;
        }
        String instanceId = args[0];
        int numberOfTasks = Integer.parseInt(args[1]);
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        try (EcsClient ecsClient = EcsClient.create();
                AutoScalingClient asClient = AutoScalingClient.create();) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            //new RunCompactionTasks(instanceProperties, ecsClient, asClient)
            //        .runToMeetTargetTasks(numberOfTasks);
        } finally {
            sqsClient.shutdown();
            s3Client.shutdown();
        }
    }
}
