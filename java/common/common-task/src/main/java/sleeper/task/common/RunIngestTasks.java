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

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.AwsVpcConfiguration;
import com.amazonaws.services.ecs.model.ContainerOverride;
import com.amazonaws.services.ecs.model.LaunchType;
import com.amazonaws.services.ecs.model.NetworkConfiguration;
import com.amazonaws.services.ecs.model.PropagateTags;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.TaskOverride;
import com.amazonaws.services.sqs.AmazonSQS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.ArrayList;
import java.util.List;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_CLUSTER;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_TASK_DEFINITION_FAMILY;
import static sleeper.configuration.properties.instance.CommonProperty.ECS_SECURITY_GROUPS;
import static sleeper.configuration.properties.instance.CommonProperty.FARGATE_VERSION;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.instance.IngestProperty.MAXIMUM_CONCURRENT_INGEST_TASKS;

/**
 * Finds the number of messages on a queue, and starts up one Fargate task for each, up to a configurable maximum.
 */
public class RunIngestTasks {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunIngestTasks.class);

    private final AmazonSQS sqsClient;
    private final AmazonECS ecsClient;
    private final InstanceProperties properties;
    private final PropertiesReloader propertiesReloader;
    private final String containerName;

    public RunIngestTasks(AmazonSQS sqsClient,
            AmazonECS ecsClient,
            InstanceProperties properties,
            PropertiesReloader propertiesReloader,
            String containerName) {
        this.sqsClient = sqsClient;
        this.ecsClient = ecsClient;
        this.properties = properties;
        this.propertiesReloader = propertiesReloader;
        this.containerName = containerName;
    }

    public void run() {
        propertiesReloader.reloadIfNeeded();
        String sqsJobQueueUrl = properties.get(INGEST_JOB_QUEUE_URL);
        LOGGER.info("Queue URL is {}", sqsJobQueueUrl);
        // Find out number of messages in queue that are not being processed
        int queueSize = QueueMessageCount.withSqsClient(sqsClient).getQueueMessageCount(sqsJobQueueUrl)
                .getApproximateNumberOfMessages();
        LOGGER.debug("Queue size is {}", queueSize);
        if (0 == queueSize) {
            LOGGER.info("Finishing as queue size is 0");
            return;
        }

        // Find out number of pending and running tasks
        int numRunningAndPendingTasks = ECSTaskCount.getNumPendingAndRunningTasks(
                properties.get(INGEST_CLUSTER), ecsClient);
        LOGGER.info("Number of running and pending tasks is {}", numRunningAndPendingTasks);

        // Finish if number of running tasks is already the maximum
        int maximumRunningTasks = properties.getInt(MAXIMUM_CONCURRENT_INGEST_TASKS);
        if (numRunningAndPendingTasks == maximumRunningTasks) {
            LOGGER.info("Finishing as number of running tasks is already the maximum");
            return;
        }

        // Calculate maximum number of tasks to create
        int maxNumTasksToCreate = maximumRunningTasks - numRunningAndPendingTasks;
        LOGGER.debug("Maximum number of tasks to create is {}", maxNumTasksToCreate);

        // Create 1 task per ingest jobs up to the maximum number of tasks to create
        int numberOfTasksToCreate = Math.min(queueSize, maxNumTasksToCreate);

        List<String> args = new ArrayList<>();
        args.add(properties.get(CONFIG_BUCKET));

        ContainerOverride containerOverride = new ContainerOverride()
                .withName(containerName)
                .withCommand(args);

        TaskOverride override = new TaskOverride()
                .withContainerOverrides(containerOverride);

        AwsVpcConfiguration vpcConfiguration = new AwsVpcConfiguration()
                .withSubnets(properties.getList(SUBNETS))
                .withSecurityGroups(properties.getList(ECS_SECURITY_GROUPS));

        NetworkConfiguration networkConfiguration = new NetworkConfiguration()
                .withAwsvpcConfiguration(vpcConfiguration);

        RunTaskRequest runTaskRequest = new RunTaskRequest()
                .withCluster(properties.get(INGEST_CLUSTER))
                .withLaunchType(LaunchType.FARGATE)
                .withTaskDefinition(properties.get(INGEST_TASK_DEFINITION_FAMILY))
                .withNetworkConfiguration(networkConfiguration)
                .withOverrides(override)
                .withPropagateTags(PropagateTags.TASK_DEFINITION)
                .withPlatformVersion(properties.get(FARGATE_VERSION));

        RunECSTasks.runTasks(ecsClient, runTaskRequest, numberOfTasksToCreate);
    }
}
