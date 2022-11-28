/*
 * Copyright 2022 Crown Copyright
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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.AwsVpcConfiguration;
import com.amazonaws.services.ecs.model.ContainerOverride;
import com.amazonaws.services.ecs.model.Failure;
import com.amazonaws.services.ecs.model.LaunchType;
import com.amazonaws.services.ecs.model.NetworkConfiguration;
import com.amazonaws.services.ecs.model.PropagateTags;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.RunTaskResult;
import com.amazonaws.services.ecs.model.TaskOverride;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.job.common.CommonJobUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

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
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_EC2_SCALING_GRACE_PERIOD;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_TASK_ARM_CPU;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_TASK_ARM_MEMORY;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_TASK_CPU_ARCHITECTURE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_TASK_X86_CPU;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_TASK_X86_MEMORY;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FARGATE_VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONCURRENT_COMPACTION_TASKS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;
import static sleeper.core.ContainerConstants.COMPACTION_CONTAINER_NAME;
import static sleeper.core.ContainerConstants.SPLITTING_COMPACTION_CONTAINER_NAME;

/**
 * Finds the number of messages on a queue, and starts up one EC2 or Fargate
 * task for
 * each, up to a configurable maximum.
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
    private final String subnet;
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
        this.subnet = instanceProperties.get(SUBNET);
        this.fargateVersion = instanceProperties.get(FARGATE_VERSION);
        this.launchType = instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE);

        String architecture = instanceProperties.get(COMPACTION_TASK_CPU_ARCHITECTURE).toUpperCase(Locale.ROOT);
        Pair<Integer, Integer> requirements = getCpuMemoryForArch(architecture, launchType, instanceProperties);

        this.scaler = new Scaler(asClient, ecsClient, autoScalingGroupName, this.clusterName,
                requirements.getLeft(),
                requirements.getRight(),
                Duration.ofSeconds(instanceProperties.getInt(COMPACTION_EC2_SCALING_GRACE_PERIOD)));
    }

    /**
     * Retrieves architecture specific CPU and memory requirements. This returns
     * a triple containing the CPU requirement in the left element and memory
     * requirement in the right element.
     *
     * @param architecture CPU architecture
     * @param launchType the container launch type
     * @param instanceProperties Sleeper instance properties
     * @return CPU and memory requirements as per the CPU architecture
     */
    private static Pair<Integer, Integer> getCpuMemoryForArch(String architecture,
            String launchType,
            InstanceProperties instanceProperties) {
        int cpu;
        int memoryLimitMiB;
        if (architecture.startsWith("ARM")) {
            cpu = instanceProperties.getInt(COMPACTION_TASK_ARM_CPU);
            memoryLimitMiB = instanceProperties.getInt(COMPACTION_TASK_ARM_MEMORY);
        } else {
            cpu = instanceProperties.getInt(COMPACTION_TASK_X86_CPU);
            memoryLimitMiB = instanceProperties.getInt(COMPACTION_TASK_X86_MEMORY);
        }

        // bit hacky: EC2s don't give 100% of their memory for container use (OS
        // headroom, system tasks, etc.) so since we want a whole GPU
        // with only 1 GPU per typical system, we have to make sure to reduce
        // the EC2 memory requirement by 5%. If we don't we end up asking for
        // 16GiB of RAM on a 16GiB box and allocation will fail.
        if (launchType.equalsIgnoreCase("EC2")) {
            memoryLimitMiB = (int) (memoryLimitMiB * 0.95);
        }

        return Pair.of(cpu, memoryLimitMiB);
    }

    public void run() throws InterruptedException {
        long startTime = System.currentTimeMillis();

        // Find out number of messages in queue that are not being processed
        int queueSize = CommonJobUtils.getNumberOfMessagesInQueue(sqsJobQueueUrl, sqsClient)
                .get(QueueAttributeName.ApproximateNumberOfMessages.toString());
        LOGGER.info("Queue size is {}", queueSize);

        // Obtain details of instances in this cluster
        Map<String, InstanceDetails> details = InstanceDetails.fetchInstanceDetails(this.clusterName, ecsClient);
        LOGGER.debug("Cluster container instances {}", details);
        Set<String> recentContainerInstanceARNs = new HashSet<>();

        if (0 == queueSize) {
            LOGGER.info("Finishing as queue size is 0");
        } else {
            int numRunningTasks = CommonJobUtils.getNumRunningTasks(clusterName, ecsClient);
            LOGGER.info("Number of running tasks is {}", numRunningTasks);

            int maxNumTasksToCreate = maximumRunningTasks - numRunningTasks;
            LOGGER.info("Maximum number of tasks to create is {}", maxNumTasksToCreate);

            // Do we need to scale out?
            int maxNumTasksThatWillBeCreated = Math.min(maxNumTasksToCreate, queueSize);
            if (launchType.equalsIgnoreCase("EC2")) {
                scaler.possiblyScaleOut(maxNumTasksThatWillBeCreated, details);
            }
            // Create 1 task for each item on the queue
            int numTasksCreated = 0;
            for (int i = 0; i < queueSize && i < maxNumTasksToCreate; i++) {
                List<String> args = new ArrayList<>();
                args.add(s3Bucket);
                args.add(type);

                ContainerOverride containerOverride = new ContainerOverride()
                        .withName(containerName)
                        .withCommand(args);

                TaskOverride override = new TaskOverride()
                        .withContainerOverrides(containerOverride);

                AwsVpcConfiguration vpcConfiguration = new AwsVpcConfiguration()
                        .withSubnets(subnet);

                NetworkConfiguration networkConfiguration = new NetworkConfiguration()
                        .withAwsvpcConfiguration(vpcConfiguration);

                RunTaskRequest runTaskRequest = new RunTaskRequest()
                        .withCluster(clusterName)
                        .withOverrides(override)
                        .withPropagateTags(PropagateTags.TASK_DEFINITION);

                String defUsed;
                if (launchType.equals("FARGATE")) {
                    defUsed = fargateTaskDefinition;
                    runTaskRequest = runTaskRequest
                            .withTaskDefinition(defUsed)
                            .withNetworkConfiguration(networkConfiguration)
                            .withPlatformVersion(fargateVersion)
                            .withLaunchType(LaunchType.FARGATE);
                } else {
                    defUsed = ec2TaskDefinition;
                    runTaskRequest = runTaskRequest
                            .withTaskDefinition(defUsed)
                            .withLaunchType(LaunchType.EC2);
                }

                try {
                    RunTaskResult runTaskResult = ecsClient.runTask(runTaskRequest);
                    LOGGER.info(
                            "Submitted RunTaskRequest (cluster = {}, type = {}, container name = {}, task definition = {})",
                            clusterName, launchType, containerName, defUsed);
                    runTaskResult.getTasks().stream()
                            .filter(task -> task.getContainerInstanceArn() != null)
                            .forEach(task -> {
                                recentContainerInstanceARNs.add(task.getContainerInstanceArn());
                            });

                    if (runTaskResult.getFailures().size() > 0) {
                        LOGGER.warn("Run task request has {} failures", runTaskResult.getFailures().size());
                        for (Failure f : runTaskResult.getFailures()) {
                            LOGGER.error("Failure: ARN {} Reason {} Detail {}", f.getArn(), f.getReason(),
                                    f.getDetail());
                        }
                        break;
                    }
                    numTasksCreated++;

                    // This lambda is triggered every minute so abort once get
                    // close to 1 minute
                    if (System.currentTimeMillis() - startTime > 50 * 1000L) {
                        LOGGER.info("RunTasks has been running for more than 50 seconds, aborting");
                        break;
                    }

                    if (0 == numTasksCreated % 10) {
                        // Sleep for 10 seconds - API allows 1 job per second
                        // with a burst of 10 jobs in
                        // a second
                        // so run 10 every 11 seconds for safety
                        LOGGER.info("Sleeping for 11 seconds as 10 tasks have been created");
                        Thread.sleep(11000L);
                    }
                } catch (AmazonClientException e) {
                    LOGGER.error("Couldn't launch tasks", e);
                }
            }
        }

        try {
            if (launchType.equalsIgnoreCase("EC2")) {
                scaler.possiblyScaleIn(this.ec2TaskDefinition, details, recentContainerInstanceARNs);
            }
        } catch (AmazonClientException e) {
            LOGGER.error("Scale-in exception", e);
        }
    }
}
