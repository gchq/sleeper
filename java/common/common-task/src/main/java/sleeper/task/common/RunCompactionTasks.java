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
package sleeper.task.common;

import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClientBuilder;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.ecs.model.AwsVpcConfiguration;
import com.amazonaws.services.ecs.model.ContainerOverride;
import com.amazonaws.services.ecs.model.LaunchType;
import com.amazonaws.services.ecs.model.NetworkConfiguration;
import com.amazonaws.services.ecs.model.PropagateTags;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.TaskOverride;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_EC2_DEFINITION_FAMILY;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_FARGATE_DEFINITION_FAMILY;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.ECS_SECURITY_GROUPS;
import static sleeper.configuration.properties.instance.CommonProperty.FARGATE_VERSION;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.configuration.properties.instance.CompactionProperty.MAXIMUM_CONCURRENT_COMPACTION_TASKS;
import static sleeper.core.ContainerConstants.COMPACTION_CONTAINER_NAME;

/**
 * Finds the number of messages on a queue, and starts up one EC2 or Fargate task for each, up to a
 * configurable maximum.
 */
public class RunCompactionTasks {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunCompactionTasks.class);

    private final InstanceProperties instanceProperties;
    private final TaskCounts taskCounts;
    private final HostScaler hostScaler;
    private final TaskLauncher taskLauncher;

    public RunCompactionTasks(
            InstanceProperties instanceProperties, AmazonECS ecsClient, AmazonAutoScaling asClient) {
        this(instanceProperties,
                () -> ECSTaskCount.getNumPendingAndRunningTasks(instanceProperties.get(COMPACTION_CLUSTER), ecsClient),
                createEC2Scaler(instanceProperties, asClient, ecsClient),
                (numberOfTasks, checkAbort) -> launchTasks(ecsClient, instanceProperties, numberOfTasks, checkAbort));
    }

    public RunCompactionTasks(
            InstanceProperties instanceProperties, TaskCounts taskCounts, HostScaler hostScaler, TaskLauncher taskLauncher) {
        this.instanceProperties = instanceProperties;
        this.taskCounts = taskCounts;
        this.hostScaler = hostScaler;
        this.taskLauncher = taskLauncher;
    }

    public interface TaskCounts {
        int getRunningAndPending();
    }

    public interface TaskLauncher {
        void launchTasks(int numberOfTasksToCreate, BooleanSupplier checkAbort);
    }

    public interface HostScaler {
        void scaleTo(int numberContainers);
    }

    public void run(QueueMessageCount.Client queueMessageCount) {
        long startTime = System.currentTimeMillis();
        String sqsJobQueueUrl = instanceProperties.get(COMPACTION_JOB_QUEUE_URL);
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
        scaleToHostsAndLaunchTasks(targetTasks, numberOfTasksToCreate,
                () -> {
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

    public void runToMeetTargetTasks(int targetCount) {
        int numRunningAndPendingTasks = taskCounts.getRunningAndPending();
        LOGGER.info("Number of running and pending tasks is {}", numRunningAndPendingTasks);
        int numberOfTasksToCreate = targetCount - numRunningAndPendingTasks;
        scaleToHostsAndLaunchTasks(targetCount, numberOfTasksToCreate, () -> false);
    }

    private void scaleToHostsAndLaunchTasks(int targetTasks, int createTasks, BooleanSupplier checkAbort) {
        LOGGER.info("Target number of tasks is {}", targetTasks);
        LOGGER.info("Tasks to create is {}", createTasks);
        if (createTasks < 1) {
            LOGGER.info("Finishing as no new tasks are needed");
            return;
        }
        hostScaler.scaleTo(targetTasks);
        taskLauncher.launchTasks(createTasks, checkAbort);
    }

    private static HostScaler createEC2Scaler(InstanceProperties instanceProperties, AmazonAutoScaling asClient, AmazonECS ecsClient) {
        String launchType = instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE);
        // Only need scaler for EC2
        if (!launchType.equalsIgnoreCase("EC2")) {
            return hostCount -> {
            };
        }
        return EC2Scaler.create(instanceProperties, asClient, ecsClient)::scaleTo;
    }

    /**
     * Attempts to launch some tasks on ECS.
     *
     * @param ecsClient             Amazon ECS client
     * @param instanceProperties    Properties for instance
     * @param numberOfTasksToCreate number of tasks to create
     * @param checkAbort            a condition under which launching will be aborted
     */
    private static void launchTasks(
            AmazonECS ecsClient, InstanceProperties instanceProperties,
            int numberOfTasksToCreate, BooleanSupplier checkAbort) {
        String clusterName = instanceProperties.get(COMPACTION_CLUSTER);
        String fargateTaskDefinition = instanceProperties.get(COMPACTION_TASK_FARGATE_DEFINITION_FAMILY);
        String ec2TaskDefinition = instanceProperties.get(COMPACTION_TASK_EC2_DEFINITION_FAMILY);
        String fargateVersion = instanceProperties.get(FARGATE_VERSION);
        String launchType = instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE);
        TaskOverride override = createOverride(List.of(instanceProperties.get(CONFIG_BUCKET)), COMPACTION_CONTAINER_NAME);
        NetworkConfiguration networkConfiguration = networkConfig(instanceProperties);
        String defUsed = (launchType.equalsIgnoreCase("FARGATE")) ? fargateTaskDefinition : ec2TaskDefinition;
        RunTaskRequest runTaskRequest = createRunTaskRequest(
                clusterName, launchType, fargateVersion,
                override, networkConfiguration, defUsed);

        RunECSTasks.runTasks(builder -> builder
                .ecsClient(ecsClient)
                .runTaskRequest(runTaskRequest)
                .numberOfTasksToCreate(numberOfTasksToCreate)
                .checkAbort(checkAbort));
    }

    /**
     * Create the container networking configuration.
     *
     * @param  instanceProperties the instance properties
     * @return                    task network configuration
     */
    private static NetworkConfiguration networkConfig(InstanceProperties instanceProperties) {
        AwsVpcConfiguration vpcConfiguration = new AwsVpcConfiguration()
                .withSubnets(instanceProperties.getList(SUBNETS))
                .withSecurityGroups(instanceProperties.getList(ECS_SECURITY_GROUPS));

        return new NetworkConfiguration()
                .withAwsvpcConfiguration(vpcConfiguration);
    }

    /**
     * Create the container definition overrides for the task launch.
     *
     * @param  args          container runtime args
     * @param  containerName name of container used for overriding
     * @return               the container definition overrides
     */
    private static TaskOverride createOverride(List<String> args, String containerName) {
        ContainerOverride containerOverride = new ContainerOverride()
                .withName(containerName)
                .withCommand(args);

        return new TaskOverride()
                .withContainerOverrides(containerOverride);
    }

    /**
     * Creates a new task request that can be passed to ECS.
     *
     * @param  clusterName              ECS cluster
     * @param  launchType               either FARGATE or EC2
     * @param  fargateVersion           version string if running on Fargate
     * @param  override                 specific container overrides
     * @param  networkConfiguration     the networking configuration for the container
     * @param  defUsed                  which task definition to use
     * @return                          the request for ECS
     * @throws IllegalArgumentException if <code>launchType</code> is FARGATE and version is null
     */
    private static RunTaskRequest createRunTaskRequest(String clusterName, String launchType, String fargateVersion,
            TaskOverride override, NetworkConfiguration networkConfiguration, String defUsed) {
        RunTaskRequest runTaskRequest = new RunTaskRequest()
                .withCluster(clusterName)
                .withOverrides(override)
                .withTaskDefinition(defUsed)
                .withPropagateTags(PropagateTags.TASK_DEFINITION);

        if (launchType.equals("FARGATE")) {
            Objects.requireNonNull(fargateVersion, "fargateVersion cannot be null");
            return runTaskRequest
                    .withPlatformVersion(fargateVersion)
                    .withNetworkConfiguration(networkConfiguration)
                    .withLaunchType(LaunchType.FARGATE);
        } else {
            return runTaskRequest
                    .withLaunchType(LaunchType.EC2);
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: <instance-id> <number-of-tasks>");
            return;
        }
        String instanceId = args[0];
        int numberOfTasks = Integer.parseInt(args[1]);
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        AmazonECS ecsClient = AmazonECSClientBuilder.defaultClient();
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonAutoScaling asClient = AmazonAutoScalingClientBuilder.defaultClient();
        try {
            InstanceProperties instanceProperties = new InstanceProperties();
            instanceProperties.loadFromS3GivenInstanceId(s3Client, instanceId);
            new RunCompactionTasks(instanceProperties, ecsClient, asClient)
                    .runToMeetTargetTasks(numberOfTasks);
        } finally {
            sqsClient.shutdown();
            ecsClient.shutdown();
            s3Client.shutdown();
            asClient.shutdown();
        }
    }
}
