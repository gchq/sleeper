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

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.AwsVpcConfiguration;
import com.amazonaws.services.ecs.model.ContainerOverride;
import com.amazonaws.services.ecs.model.LaunchType;
import com.amazonaws.services.ecs.model.NetworkConfiguration;
import com.amazonaws.services.ecs.model.PropagateTags;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.RunTaskResult;
import com.amazonaws.services.ecs.model.TaskOverride;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.job.common.CommonJobUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_TASK_DEFINITION_FAMILY;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_DEFINITION_FAMILY;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FARGATE_VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONCURRENT_COMPACTION_TASKS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;
import static sleeper.core.ContainerConstants.COMPACTION_CONTAINER_NAME;
import static sleeper.core.ContainerConstants.SPLITTING_COMPACTION_CONTAINER_NAME;

/**
 * Finds the number of messages on a queue, and starts up one Fargate task for
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
    private final String taskDefinition;
    private final int maximumRunningTasks;
    private final String subnet;
    private final String fargateVersion;

    public RunTasks(AmazonSQS sqsClient,
                    AmazonECS ecsClient,
                    AmazonS3 s3Client,
                    String s3Bucket,
                    String type) throws IOException {
        this.sqsClient = sqsClient;
        this.ecsClient = ecsClient;
        this.s3Bucket = s3Bucket;
        this.type = type;

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, s3Bucket);

        if (type.equals("compaction")) {
            this.sqsJobQueueUrl = instanceProperties.get(COMPACTION_JOB_QUEUE_URL);
            this.clusterName = instanceProperties.get(COMPACTION_CLUSTER);
            this.containerName = COMPACTION_CONTAINER_NAME;
            this.taskDefinition = instanceProperties.get(COMPACTION_TASK_DEFINITION_FAMILY);
        } else if (type.equals("splittingcompaction")) {
            this.sqsJobQueueUrl = instanceProperties.get(SPLITTING_COMPACTION_JOB_QUEUE_URL);
            this.clusterName = instanceProperties.get(SPLITTING_COMPACTION_CLUSTER);
            this.containerName = SPLITTING_COMPACTION_CONTAINER_NAME;
            this.taskDefinition = instanceProperties.get(SPLITTING_COMPACTION_TASK_DEFINITION_FAMILY);
        } else {
            throw new RuntimeException("type should be 'compaction' or 'splittingcompaction'");
        }
        this.maximumRunningTasks = instanceProperties.getInt(MAXIMUM_CONCURRENT_COMPACTION_TASKS);
        this.subnet = instanceProperties.get(SUBNET);
        this.fargateVersion = instanceProperties.get(FARGATE_VERSION);
    }

    public void run() throws InterruptedException {
        long startTime = System.currentTimeMillis();

        // Find out number of messages in queue that are not being processed
        int queueSize = CommonJobUtils.getNumberOfMessagesInQueue(sqsJobQueueUrl, sqsClient)
                .get(QueueAttributeName.ApproximateNumberOfMessages.toString());
        LOGGER.info("Queue size is {}", queueSize);
        if (0 == queueSize) {
            LOGGER.info("Finishing as queue size is 0");
            return;
        }

        int numRunningTasks = CommonJobUtils.getNumRunningTasks(clusterName, ecsClient);
        LOGGER.info("Number of running tasks is {}", numRunningTasks);

        int maxNumTasksToCreate = maximumRunningTasks - numRunningTasks;
        LOGGER.info("Maximum number of tasks to create is {}", maxNumTasksToCreate);

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
                    .withLaunchType(LaunchType.EC2) //TODO Support both types here 
                    .withTaskDefinition(taskDefinition)
//                    .withNetworkConfiguration(networkConfiguration) //TODO this should only be enabled on FARGATE tasks
                    .withOverrides(override)
                    .withPropagateTags(PropagateTags.TASK_DEFINITION);
                    //.withPlatformVersion(fargateVersion);

            RunTaskResult runTaskResult = ecsClient.runTask(runTaskRequest);
            LOGGER.info("Submitted RunTaskRequest (cluster = {}, container name = {}, task definition = {})",
                    clusterName, containerName, taskDefinition);
            if (runTaskResult.getFailures().size() > 0) {
                LOGGER.warn("Run task request has {} failures", runTaskResult.getFailures().size());
                return;
            }
            numTasksCreated++;

            // This lambda is triggered every minute so abort once get close to 1 minute
            if (System.currentTimeMillis() - startTime > 50 * 1000L) {
                LOGGER.info("RunTasks has been running for more than 50 seconds, aborting");
                return;
            }

            if (0 == numTasksCreated % 10) {
                // Sleep for 10 seconds - API allows 1 job per second with a burst of 10 jobs in a second
                // so run 10 every 11 seconds for safety
                LOGGER.info("Sleeping for 11 seconds as 10 tasks have been created");
                Thread.sleep(11000L);
            }
        }
    }
}
