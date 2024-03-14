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
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.Requirements;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_AUTO_SCALING_GROUP;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_EC2_DEFINITION_FAMILY;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_FARGATE_DEFINITION_FAMILY;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FARGATE_VERSION;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_CPU_ARCHITECTURE;
import static sleeper.configuration.properties.instance.CompactionProperty.MAXIMUM_CONCURRENT_COMPACTION_TASKS;
import static sleeper.configuration.properties.instance.InstanceProperties.getConfigBucketFromInstanceId;
import static sleeper.core.ContainerConstants.COMPACTION_CONTAINER_NAME;

/**
 * Finds the number of messages on a queue, and starts up one EC2 or Fargate task for each, up to a
 * configurable maximum.
 */
public class RunCompactionTasks {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunCompactionTasks.class);

    private final InstanceProperties instanceProperties;
    private final String sqsJobQueueUrl;
    private final String clusterName;
    private final String launchType;
    private final int maximumRunningTasks;
    private final TaskCounts taskCounts;
    private final Scaler scaler;
    private final TaskLauncher launchTasks;

    public RunCompactionTasks(
            InstanceProperties instanceProperties, AmazonECS ecsClient, AmazonAutoScaling asClient) {
        this(instanceProperties,
                (clusterName) -> ECSTaskCount.getNumPendingAndRunningTasks(clusterName, ecsClient),
                createEC2Scaler(instanceProperties, asClient, ecsClient),
                (startTime, numberOfTasksToCreate) -> launchTasks(ecsClient, instanceProperties, startTime, numberOfTasksToCreate));
    }

    public RunCompactionTasks(
            InstanceProperties instanceProperties, TaskCounts taskCounts, Scaler scaler, TaskLauncher launchTasks) {
        this.instanceProperties = instanceProperties;
        this.taskCounts = taskCounts;
        this.scaler = scaler;
        this.launchTasks = launchTasks;
        this.sqsJobQueueUrl = instanceProperties.get(COMPACTION_JOB_QUEUE_URL);
        this.clusterName = instanceProperties.get(COMPACTION_CLUSTER);
        this.maximumRunningTasks = instanceProperties.getInt(MAXIMUM_CONCURRENT_COMPACTION_TASKS);
        this.launchType = instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE);
    }

    public interface TaskCounts {
        int getRunningAndPending(String clusterName);
    }

    public interface TaskLauncher {
        void launchTasks(long startTime, int numberOfTasksToCreate);
    }

    public interface Scaler {
        void scaleTo(String asGroupName, int numberContainers);
    }

    public void run(QueueMessageCount.Client queueMessageCount) {
        long startTime = System.currentTimeMillis();
        LOGGER.info("Queue URL is {}", sqsJobQueueUrl);
        // Find out number of messages in queue that are not being processed
        int queueSize = queueMessageCount.getQueueMessageCount(sqsJobQueueUrl)
                .getApproximateNumberOfMessages();
        LOGGER.info("Queue size is {}", queueSize);
        // Request 1 task for each item on the queue
        run(startTime, queueSize);
    }

    public void run(int requestedTasks) {
        run(System.currentTimeMillis(), requestedTasks);
    }

    private void run(long startTime, int requestedTasks) {
        if (requestedTasks == 0) {
            LOGGER.info("Finishing as number of tasks requested was 0");
            return;
        }
        // Find out number of pending and running tasks
        int numRunningAndPendingTasks = taskCounts.getRunningAndPending(clusterName);
        LOGGER.info("Number of running and pending tasks is {}", numRunningAndPendingTasks);

        if (numRunningAndPendingTasks >= maximumRunningTasks) {
            LOGGER.info("Finishing as maximum running tasks of {} has been reached", maximumRunningTasks);
            return;
        }
        int maxNumTasksToCreate = maximumRunningTasks - numRunningAndPendingTasks;
        LOGGER.info("Maximum concurrent tasks is {}", maximumRunningTasks);
        LOGGER.info("Maximum number of tasks that can be created is {}", maxNumTasksToCreate);
        int numberOfTasksToCreate = Math.min(requestedTasks, maxNumTasksToCreate);
        if (launchType.equalsIgnoreCase("EC2")) {
            int totalTasks = numberOfTasksToCreate + numRunningAndPendingTasks;
            LOGGER.info("Total number of tasks if all launches succeed {}", totalTasks);
            scaler.scaleTo(instanceProperties.get(COMPACTION_AUTO_SCALING_GROUP), totalTasks);
        }

        launchTasks.launchTasks(startTime, numberOfTasksToCreate);
    }

    private static Scaler createEC2Scaler(InstanceProperties instanceProperties, AmazonAutoScaling asClient, AmazonECS ecsClient) {
        String launchType = instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE);
        String architecture = instanceProperties.get(COMPACTION_TASK_CPU_ARCHITECTURE).toUpperCase(Locale.ROOT);
        Pair<Integer, Integer> requirements = Requirements.getArchRequirements(architecture, launchType, instanceProperties);
        // Bit hacky: EC2s don't give 100% of their memory for container use (OS
        // headroom, system tasks, etc.) so we have to make sure to reduce
        // the EC2 memory requirement by 5%. If we don't we end up asking for
        // 16GiB of RAM on a 16GiB box for example and container allocation will fail.
        if (launchType.equalsIgnoreCase("EC2")) {
            requirements = Pair.of(requirements.getLeft(), (int) (requirements.getRight() * 0.95));
        }

        return new EC2Scaler(asClient, ecsClient, instanceProperties.get(COMPACTION_AUTO_SCALING_GROUP),
                instanceProperties.get(COMPACTION_CLUSTER), requirements.getLeft(), requirements.getRight())::scaleTo;
    }

    /**
     * Attempts to launch some tasks on ECS.
     *
     * @param ecsClient             Amazon ECS client
     * @param instanceProperties    Properties for instance
     * @param startTime             start time of Lambda
     * @param numberOfTasksToCreate number of tasks to create
     */
    private static void launchTasks(AmazonECS ecsClient, InstanceProperties instanceProperties, long startTime, int numberOfTasksToCreate) {
        String clusterName = instanceProperties.get(COMPACTION_CLUSTER);
        String fargateTaskDefinition = instanceProperties.get(COMPACTION_TASK_FARGATE_DEFINITION_FAMILY);
        String ec2TaskDefinition = instanceProperties.get(COMPACTION_TASK_EC2_DEFINITION_FAMILY);
        String fargateVersion = instanceProperties.get(FARGATE_VERSION);
        String launchType = instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE);
        TaskOverride override = createOverride(List.of(instanceProperties.get(CONFIG_BUCKET)), COMPACTION_CONTAINER_NAME);
        NetworkConfiguration networkConfiguration = networkConfig(instanceProperties.getList(SUBNETS));
        String defUsed = (launchType.equalsIgnoreCase("FARGATE")) ? fargateTaskDefinition : ec2TaskDefinition;
        RunTaskRequest runTaskRequest = createRunTaskRequest(
                clusterName, launchType, fargateVersion,
                override, networkConfiguration, defUsed);

        RunECSTasks.runTasks(builder -> builder
                .ecsClient(ecsClient)
                .runTaskRequest(runTaskRequest)
                .numberOfTasksToCreate(numberOfTasksToCreate)
                .checkAbort(() -> {
                    // This lambda is triggered every minute so abort once get
                    // close to 1 minute
                    if (System.currentTimeMillis() - startTime > 50 * 1000L) {
                        LOGGER.info("Running for more than 50 seconds, aborting");
                        return true;
                    } else {
                        return false;
                    }
                }));
    }

    /**
     * Create the container networking configuration.
     *
     * @param  subnets the subnet name
     * @return         task network configuration
     */
    private static NetworkConfiguration networkConfig(List<String> subnets) {
        AwsVpcConfiguration vpcConfiguration = new AwsVpcConfiguration()
                .withSubnets(subnets);

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
                    .withNetworkConfiguration(networkConfiguration)
                    .withPlatformVersion(fargateVersion)
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
        String s3Bucket = getConfigBucketFromInstanceId(instanceId);
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        AmazonECS ecsClient = AmazonECSClientBuilder.defaultClient();
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonAutoScaling asClient = AmazonAutoScalingClientBuilder.defaultClient();
        try {
            InstanceProperties instanceProperties = new InstanceProperties();
            instanceProperties.loadFromS3(s3Client, s3Bucket);
            new RunCompactionTasks(instanceProperties, ecsClient, asClient)
                    .run(numberOfTasks);
        } finally {
            sqsClient.shutdown();
            ecsClient.shutdown();
            s3Client.shutdown();
            asClient.shutdown();
        }
    }
}
