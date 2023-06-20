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
package sleeper.compaction.jobexecution;

import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.AwsVpcConfiguration;
import com.amazonaws.services.ecs.model.ContainerOverride;
import com.amazonaws.services.ecs.model.LaunchType;
import com.amazonaws.services.ecs.model.NetworkConfiguration;
import com.amazonaws.services.ecs.model.PropagateTags;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.TaskOverride;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.Requirements;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.job.common.CommonJobUtils;
import sleeper.job.common.QueueMessageCount;
import sleeper.job.common.RunECSTasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_AUTO_SCALING_GROUP;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_TASK_EC2_DEFINITION_FAMILY;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_TASK_FARGATE_DEFINITION_FAMILY;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_AUTO_SCALING_GROUP;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_EC2_DEFINITION_FAMILY;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_FARGATE_DEFINITION_FAMILY;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_TASK_CPU_ARCHITECTURE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FARGATE_VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONCURRENT_COMPACTION_TASKS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNETS;
import static sleeper.core.ContainerConstants.COMPACTION_CONTAINER_NAME;
import static sleeper.core.ContainerConstants.SPLITTING_COMPACTION_CONTAINER_NAME;

/**
 * Finds the number of messages on a queue, and starts up one EC2 or Fargate task for each, up to a
 * configurable maximum.
 */
public class RunTasks {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunTasks.class);

    private final AmazonSQS sqsClient;
    private final AmazonECS ecsClient;
    private final String s3Bucket;
    private final String type;
    private final String sqsJobQueueUrl;
    private final String clusterName;
    private final String containerName;
    private final String fargateTaskDefinition;
    private final String ec2TaskDefinition;
    private final String launchType;
    private final int maximumRunningTasks;
    private final List<String> subnets;
    private final String fargateVersion;
    private final Scaler scaler;

    public RunTasks(AmazonSQS sqsClient,
                    AmazonECS ecsClient,
                    AmazonS3 s3Client,
                    AmazonAutoScaling asClient,
                    String s3Bucket,
                    String type) throws IOException {
        this.sqsClient = sqsClient;
        this.ecsClient = ecsClient;
        this.s3Bucket = s3Bucket;
        this.type = type;

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, s3Bucket);
        String autoScalingGroupName;
        if (type.equals("compaction")) {
            this.sqsJobQueueUrl = instanceProperties.get(COMPACTION_JOB_QUEUE_URL);
            this.clusterName = instanceProperties.get(COMPACTION_CLUSTER);
            this.containerName = COMPACTION_CONTAINER_NAME;
            this.fargateTaskDefinition = instanceProperties.get(COMPACTION_TASK_FARGATE_DEFINITION_FAMILY);
            this.ec2TaskDefinition = instanceProperties.get(COMPACTION_TASK_EC2_DEFINITION_FAMILY);
            autoScalingGroupName = instanceProperties.get(COMPACTION_AUTO_SCALING_GROUP);
        } else if (type.equals("splittingcompaction")) {
            this.sqsJobQueueUrl = instanceProperties.get(SPLITTING_COMPACTION_JOB_QUEUE_URL);
            this.clusterName = instanceProperties.get(SPLITTING_COMPACTION_CLUSTER);
            this.containerName = SPLITTING_COMPACTION_CONTAINER_NAME;
            this.fargateTaskDefinition = instanceProperties.get(SPLITTING_COMPACTION_TASK_FARGATE_DEFINITION_FAMILY);
            this.ec2TaskDefinition = instanceProperties.get(SPLITTING_COMPACTION_TASK_EC2_DEFINITION_FAMILY);
            autoScalingGroupName = instanceProperties.get(SPLITTING_COMPACTION_AUTO_SCALING_GROUP);
        } else {
            throw new RuntimeException("type should be 'compaction' or 'splittingcompaction'");
        }
        this.maximumRunningTasks = instanceProperties.getInt(MAXIMUM_CONCURRENT_COMPACTION_TASKS);
        this.subnets = instanceProperties.getList(SUBNETS);
        this.fargateVersion = instanceProperties.get(FARGATE_VERSION);
        this.launchType = instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE);

        String architecture = instanceProperties.get(COMPACTION_TASK_CPU_ARCHITECTURE).toUpperCase(Locale.ROOT);
        Pair<Integer, Integer> requirements = Requirements.getArchRequirements(architecture, launchType,
                instanceProperties);

        // Bit hacky: EC2s don't give 100% of their memory for container use (OS
        // headroom, system tasks, etc.) so we have to make sure to reduce
        // the EC2 memory requirement by 5%. If we don't we end up asking for
        // 16GiB of RAM on a 16GiB box for example and container allocation will fail.
        if (launchType.equalsIgnoreCase("EC2")) {
            requirements = Pair.of(requirements.getLeft(), (int) (requirements.getRight() * 0.95));
        }

        this.scaler = new Scaler(asClient, ecsClient, autoScalingGroupName, this.clusterName,
                requirements.getLeft(),
                requirements.getRight());
    }

    public void run() {
        long startTime = System.currentTimeMillis();
        LOGGER.info("Queue URL is {}", sqsJobQueueUrl);
        // Find out number of messages in queue that are not being processed
        int queueSize = QueueMessageCount.withSqsClient(sqsClient).getQueueMessageCount(sqsJobQueueUrl)
                .getApproximateNumberOfMessages();
        LOGGER.info("Queue size is {}", queueSize);

        // Find out number of pending and running tasks
        int numRunningAndPendingTasks = CommonJobUtils.getNumPendingAndRunningTasks(clusterName, ecsClient);
        LOGGER.info("Number of running and pending tasks is {}", numRunningAndPendingTasks);

        int maxNumTasksToCreate = maximumRunningTasks - numRunningAndPendingTasks;
        LOGGER.info("Maximum number of tasks to create is {}", maxNumTasksToCreate);

        // Do we need to scale?
        if (launchType.equalsIgnoreCase("EC2")) {
            int maxNumTasksThatWillBeCreated = Math.min(maxNumTasksToCreate, queueSize);
            int totalTasks = maxNumTasksThatWillBeCreated + numRunningAndPendingTasks;
            LOGGER.info("Total number of tasks if all launches succeed {}", totalTasks);
            scaler.scaleTo(totalTasks);
        }

        if (0 == queueSize) {
            LOGGER.info("Finishing as queue size is 0");
        } else {
            List<String> args = new ArrayList<>();
            args.add(s3Bucket);
            args.add(type);
            TaskOverride override = createOverride(args, containerName);
            NetworkConfiguration networkConfiguration = networkConfig(subnets);

            // Create 1 task for each item on the queue
            launchTasks(startTime, queueSize, maxNumTasksToCreate, override, networkConfiguration);
        }
    }

    /**
     * Attempts to launch some tasks on ECS.
     *
     * @param startTime            start time of Lambda
     * @param queueSize            length of SQS queue
     * @param maxNumTasksToCreate  number of tasks to attempt to launch
     * @param override             other container overrides
     * @param networkConfiguration container network configuration
     */
    private void launchTasks(long startTime, int queueSize, int maxNumTasksToCreate,
                             TaskOverride override, NetworkConfiguration networkConfiguration) {

        int numberOfTasksToCreate = Math.min(queueSize, maxNumTasksToCreate);
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
                        LOGGER.info("RunTasks has been running for more than 50 seconds, aborting");
                        return true;
                    } else {
                        return false;
                    }
                }));
    }

    /**
     * Create the container networking configuration.
     *
     * @param subnets the subnet name
     * @return task network configuration
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
     * @param args          container runtime args
     * @param containerName name of container used for overriding
     * @return the container definition overrides
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
     * @param clusterName          ECS cluster
     * @param launchType           either FARGATE or EC2
     * @param fargateVersion       version string if running on Fargate
     * @param override             specific container overrides
     * @param networkConfiguration the networking configuration for the container
     * @param defUsed              which task definition to use
     * @return the request for ECS
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
}
