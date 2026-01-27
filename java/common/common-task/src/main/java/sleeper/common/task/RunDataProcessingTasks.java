/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.common.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.autoscaling.AutoScalingClient;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.ecs.model.AwsVpcConfiguration;
import software.amazon.awssdk.services.ecs.model.ContainerOverride;
import software.amazon.awssdk.services.ecs.model.LaunchType;
import software.amazon.awssdk.services.ecs.model.NetworkConfiguration;
import software.amazon.awssdk.services.ecs.model.PropagateTags;
import software.amazon.awssdk.services.ecs.model.RunTaskRequest;
import software.amazon.awssdk.services.ecs.model.TaskOverride;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.CompactionECSLaunchType;

import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;

import static sleeper.core.ContainerConstants.BULK_EXPORT_CONTAINER_NAME;
import static sleeper.core.ContainerConstants.COMPACTION_CONTAINER_NAME;
import static sleeper.core.properties.instance.BulkExportProperty.MAXIMUM_CONCURRENT_BULK_EXPORT_TASKS;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_CLUSTER;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_TASK_FARGATE_DEFINITION_FAMILY;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_EC2_DEFINITION_FAMILY;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_FARGATE_DEFINITION_FAMILY;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ECS_SECURITY_GROUP;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_BULK_EXPORT_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.FARGATE_VERSION;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.core.properties.instance.CompactionProperty.MAXIMUM_CONCURRENT_COMPACTION_TASKS;

/**
 * Finds the number of messages on a queue, and starts up one EC2 or Fargate task for each, up to a
 * configurable maximum.
 */
public class RunDataProcessingTasks {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunDataProcessingTasks.class);
    private final TaskHostScaler hostScaler;
    private final String sqsJobQueueUrl;
    private final TaskCounts taskCounts;
    private final TaskLauncher taskLauncher;
    private final int maximumRunningTasks;

    private RunDataProcessingTasks(Builder builder) {
        this.hostScaler = builder.hostScaler;
        this.sqsJobQueueUrl = builder.sqsJobQueueUrl;
        this.taskCounts = builder.taskCounts;
        this.taskLauncher = builder.taskLauncher;
        this.maximumRunningTasks = builder.maximumRunningTasks;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static RunDataProcessingTasks createForBulkExport(InstanceProperties instanceProperties, EcsClient ecsClient) {
        String clusterName = instanceProperties.get(BULK_EXPORT_CLUSTER);
        return builder()
                .hostScaler(new BulkExportTaskHostScaler())
                .sqsJobQueueUrl(instanceProperties.get(LEAF_PARTITION_BULK_EXPORT_QUEUE_URL))
                .taskCounts(() -> ECSTaskCount.getNumPendingAndRunningTasks(clusterName, ecsClient))
                .taskLauncher((numberOfTasks, checkAbort) -> launchTasksForBulkExport(ecsClient, instanceProperties, BULK_EXPORT_CONTAINER_NAME,
                        clusterName, LaunchType.FARGATE,
                        instanceProperties.get(BULK_EXPORT_TASK_FARGATE_DEFINITION_FAMILY),
                        numberOfTasks, checkAbort))
                .maximumRunningTasks(instanceProperties.getInt(MAXIMUM_CONCURRENT_BULK_EXPORT_TASKS))
                .build();
    }

    public static RunDataProcessingTasks createForCompactions(InstanceProperties instanceProperties, EcsClient ecsClient, AutoScalingClient asClient, Ec2Client ec2Client) {
        String clusterName = instanceProperties.get(COMPACTION_CLUSTER);
        return builderForCompactions(instanceProperties)
                .hostScaler(EC2Scaler.create(instanceProperties, asClient, ec2Client))
                .taskCounts(() -> ECSTaskCount.getNumPendingAndRunningTasks(clusterName, ecsClient))
                .taskLauncher((numberOfTasks, checkAbort) -> launchTasksForCompaction(ecsClient, instanceProperties, COMPACTION_CONTAINER_NAME, clusterName,
                        instanceProperties.getEnumValue(COMPACTION_ECS_LAUNCHTYPE, CompactionECSLaunchType.class),
                        instanceProperties.get(COMPACTION_TASK_FARGATE_DEFINITION_FAMILY), numberOfTasks, checkAbort))
                .build();
    }

    public static Builder builderForCompactions(InstanceProperties instanceProperties) {
        return builder()
                .sqsJobQueueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL))
                .maximumRunningTasks(instanceProperties.getInt(MAXIMUM_CONCURRENT_COMPACTION_TASKS));
    }

    public interface TaskCounts {
        int getRunningAndPending();
    }

    public interface TaskLauncher {
        void launchTasks(int numberOfTasksToCreate, BooleanSupplier checkAbort);
    }

    public void run(QueueMessageCount.Client queueMessageCount) {
        long startTime = System.currentTimeMillis();
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

    /**
     * Ensure that a given number of compaction tasks are running.
     *
     * If there are currently less tasks running, then new ones will be started.
     * The scaler will be invoked to ensure the ECS cluster can accommodate the necessary new tasks
     * (may be a delay in launching instances, so initial task launch may fail).
     *
     * @param desiredTaskCount the number of tasks to ensure are running
     */
    public void runToMeetTargetTasks(int desiredTaskCount) {
        int numRunningAndPendingTasks = taskCounts.getRunningAndPending();
        LOGGER.info("Number of running and pending tasks is {}", numRunningAndPendingTasks);
        int numberOfTasksToCreate = desiredTaskCount - numRunningAndPendingTasks;
        // Instruct the scaler to ensure we have room for the correct number of running tasks. We use max() here
        // because we are either at or need to grow to the desired number. If the desiredTaskCount is LESS than
        // the number of running/pending tasks, we don't want to instruct the scaler to shrink the ECS cluster.
        scaleToHostsAndLaunchTasks(Math.max(desiredTaskCount, numRunningAndPendingTasks), numberOfTasksToCreate, () -> false);
    }

    private void scaleToHostsAndLaunchTasks(int targetTasks, int createTasks, BooleanSupplier checkAbort) {
        LOGGER.info("Target number of tasks is {}", targetTasks);
        LOGGER.info("Tasks to create is {}", createTasks);
        if (targetTasks < 0) {
            throw new IllegalArgumentException("targetTasks is < 0");
        }
        hostScaler.scaleTo(targetTasks);
        if (createTasks < 1) {
            LOGGER.info("Finishing as no new tasks are needed");
            return;
        }
        taskLauncher.launchTasks(createTasks, checkAbort);
    }

    /**
     * Attempts to launch some tasks on ECS.
     *
     * @param ecsClient               the Amazon ECS client
     * @param instanceProperties      the properties for instance
     * @param containerName           the name of the container
     * @param clusterName             the name for the cluster
     * @param launchType              the launch type either FARGATE or EC2
     * @param fargateDefinitionFamily the fargate definition family
     * @param numberOfTasksToCreate   the number of tasks to create
     * @param checkAbort              a condition under which launching will be aborted
     */
    private static void launchTasksForCompaction(
            EcsClient ecsClient, InstanceProperties instanceProperties, String containerName, String clusterName,
            CompactionECSLaunchType launchType, String fargateDefinitionFamily, int numberOfTasksToCreate, BooleanSupplier checkAbort) {
        builderForRunECSTasks(ecsClient, numberOfTasksToCreate, checkAbort)
                .runTaskRequest(createCompactionTaskRequest(instanceProperties, containerName, clusterName, launchType, fargateDefinitionFamily))
                .build().runTasks();
    }

    /**
     * Attempts to launch some tasks on ECS.
     *
     * @param ecsClient               the Amazon ECS client
     * @param instanceProperties      the properties for instance
     * @param containerName           the name of the container
     * @param clusterName             the name for the cluster
     * @param launchType              the launch type either FARGATE or EC2
     * @param fargateDefinitionFamily the fargate definition family
     * @param numberOfTasksToCreate   the number of tasks to create
     * @param checkAbort              a condition under which launching will be aborted
     */
    private static void launchTasksForBulkExport(
            EcsClient ecsClient, InstanceProperties instanceProperties, String containerName, String clusterName,
            LaunchType launchType, String fargateDefinitionFamily, int numberOfTasksToCreate, BooleanSupplier checkAbort) {
        builderForRunECSTasks(ecsClient, numberOfTasksToCreate, checkAbort)
                .runTaskRequest(createBulkExportTaskRequest(instanceProperties, containerName, clusterName, fargateDefinitionFamily))
                .build().runTasks();
    }

    /**
     * Sets up the shared values in the run ECS tasks builder.
     *
     * @param  ecsClient             the Amazon ECS client
     * @param  numberOfTasksToCreate the number of tasks to create
     * @param  checkAbort            a condition under which launching will be aborted
     * @return                       the builder to have it's run task request added
     */
    private static RunECSTasks.Builder builderForRunECSTasks(EcsClient ecsClient, int numberOfTasksToCreate, BooleanSupplier checkAbort) {
        return RunECSTasks.builder()
                .ecsClient(ecsClient)
                .numberOfTasksToCreate(numberOfTasksToCreate)
                .checkAbort(checkAbort);
    }

    /**
     * Creates a new compaction task request that can be passed to ECS.
     *
     * @param  instanceProperties       the instance properties
     * @param  containerName            the name of the container
     * @param  clusterName              the name for the cluster
     * @param  launchType               the launch type either FARGATE or EC2
     * @param  fargateDefinitionFamily  the fargate definition family
     * @return                          the request for ECS
     * @throws IllegalArgumentException if <code>launchType</code> is FARGATE and version is null
     */
    private static RunTaskRequest createCompactionTaskRequest(InstanceProperties instanceProperties, String containerName, String clusterName,
            CompactionECSLaunchType launchType, String fargateDefinitionFamily) {

        RunTaskRequest runTaskRequest;

        if (launchType == CompactionECSLaunchType.FARGATE) {
            runTaskRequest = buildFargateRequest(instanceProperties, containerName, clusterName, fargateDefinitionFamily);
        } else if (launchType == CompactionECSLaunchType.EC2) {
            runTaskRequest = builderForRunTaskRequest(instanceProperties, containerName, clusterName)
                    .launchType(LaunchType.EC2)
                    .taskDefinition(instanceProperties.get(COMPACTION_TASK_EC2_DEFINITION_FAMILY))
                    .build();
        } else {
            throw new IllegalArgumentException("Unrecognised ECS launch type: " + launchType);
        }

        return runTaskRequest;
    }

    /**
     * Creates a new build export task request that can be passed to ECS.
     *
     * @param  instanceProperties       the instance properties
     * @param  containerName            the name of the container
     * @param  clusterName              the name for the cluster
     * @param  fargateDefinitionFamily  the fargate definition family
     * @return                          the request for ECS
     * @throws IllegalArgumentException if <code>launchType</code> is FARGATE and version is null
     */
    private static RunTaskRequest createBulkExportTaskRequest(InstanceProperties instanceProperties, String containerName, String clusterName,
            String fargateDefinitionFamily) {
        return buildFargateRequest(instanceProperties, containerName, clusterName, fargateDefinitionFamily);
    }

    /**
     * Builds a run task request for a fargate launch type.
     *
     * @param  instanceProperties       the instance properties
     * @param  containerName            the name of the container
     * @param  clusterName              the name for the cluster
     * @param  fargateDefinitionFamily  the fargate definition family
     * @return                          the request for ECS
     * @throws IllegalArgumentException if <code>launchType</code> is FARGATE and version is null
     */
    private static RunTaskRequest buildFargateRequest(InstanceProperties instanceProperties, String containerName, String clusterName, String fargateDefinitionFamily) {
        return builderForRunTaskRequest(instanceProperties, containerName, clusterName)
                .launchType(LaunchType.FARGATE)
                .platformVersion(Objects.requireNonNull(instanceProperties.get(FARGATE_VERSION), "fargateVersion cannot be null"))
                .networkConfiguration(networkConfig(instanceProperties))
                .taskDefinition(fargateDefinitionFamily)
                .build();
    }

    /**
     * Sets up the shared values in the run task request builder.
     *
     * @param  instanceProperties       the instance properties
     * @param  containerName            the name of the container
     * @param  clusterName              the name for the cluster
     * @return                          the request for ECS
     * @throws IllegalArgumentException if <code>launchType</code> is FARGATE and version is null
     */
    private static RunTaskRequest.Builder builderForRunTaskRequest(InstanceProperties instanceProperties, String containerName, String clusterName) {
        return RunTaskRequest.builder()
                .cluster(clusterName)
                .overrides(createOverride(instanceProperties, containerName))
                .propagateTags(PropagateTags.TASK_DEFINITION);
    }

    /**
     * Create the container definition overrides for the task launch.
     *
     * @param  instanceProperties the instance properties
     * @param  containerName      the name of the container
     * @return                    the container definition overrides
     */
    private static TaskOverride createOverride(InstanceProperties instanceProperties, String containerName) {
        ContainerOverride containerOverride = ContainerOverride.builder()
                .name(containerName)
                .command(List.of(instanceProperties.get(CONFIG_BUCKET)))
                .build();
        return TaskOverride.builder()
                .containerOverrides(containerOverride)
                .build();
    }

    /**
     * Create the container networking configuration.
     *
     * @param  instanceProperties the instance properties
     * @return                    task network configuration
     */
    private static NetworkConfiguration networkConfig(InstanceProperties instanceProperties) {
        AwsVpcConfiguration vpcConfiguration = AwsVpcConfiguration.builder()
                .subnets(instanceProperties.getList(SUBNETS))
                //Read new instance property
                .securityGroups(instanceProperties.get(ECS_SECURITY_GROUP))
                .build();
        return NetworkConfiguration.builder()
                .awsvpcConfiguration(vpcConfiguration)
                .build();
    }

    public static class Builder {
        private TaskHostScaler hostScaler;
        private String sqsJobQueueUrl;
        private TaskCounts taskCounts;
        private TaskLauncher taskLauncher;
        private int maximumRunningTasks;

        private Builder() {

        }

        public Builder hostScaler(TaskHostScaler hostScaler) {
            this.hostScaler = hostScaler;
            return this;
        }

        public Builder sqsJobQueueUrl(String sqsJobQueueUrl) {
            this.sqsJobQueueUrl = sqsJobQueueUrl;
            return this;
        }

        public Builder taskCounts(TaskCounts taskCounts) {
            this.taskCounts = taskCounts;
            return this;
        }

        public Builder taskLauncher(TaskLauncher taskLauncher) {
            this.taskLauncher = taskLauncher;
            return this;
        }

        public Builder maximumRunningTasks(int maximumRunningTasks) {
            this.maximumRunningTasks = maximumRunningTasks;
            return this;
        }

        public RunDataProcessingTasks build() {
            return new RunDataProcessingTasks(this);
        }
    }
}
