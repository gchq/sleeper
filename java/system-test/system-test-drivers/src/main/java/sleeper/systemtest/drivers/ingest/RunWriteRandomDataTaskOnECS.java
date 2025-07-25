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
package sleeper.systemtest.drivers.ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.ecs.model.AwsVpcConfiguration;
import software.amazon.awssdk.services.ecs.model.ContainerOverride;
import software.amazon.awssdk.services.ecs.model.LaunchType;
import software.amazon.awssdk.services.ecs.model.NetworkConfiguration;
import software.amazon.awssdk.services.ecs.model.PropagateTags;
import software.amazon.awssdk.services.ecs.model.RunTaskRequest;
import software.amazon.awssdk.services.ecs.model.RunTaskResponse;
import software.amazon.awssdk.services.ecs.model.TaskOverride;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.common.task.RunECSTasks;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.systemtest.configuration.SystemTestDataGenerationJob;
import sleeper.systemtest.configuration.SystemTestProperties;
import sleeper.systemtest.configuration.SystemTestPropertyValues;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;
import sleeper.systemtest.drivers.ingest.json.TasksJson;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BY_QUEUE_ROLE_ARN;
import static sleeper.core.properties.instance.CommonProperty.ECS_SECURITY_GROUPS;
import static sleeper.core.properties.instance.CommonProperty.FARGATE_VERSION;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.systemtest.configuration.SystemTestConstants.SYSTEM_TEST_CONTAINER;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_WRITERS;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_BUCKET_NAME;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_CLUSTER_NAME;
import static sleeper.systemtest.configuration.SystemTestProperty.WRITE_DATA_TASK_DEFINITION_FAMILY;

/**
 * Runs ECS tasks to write random data.
 */
public class RunWriteRandomDataTaskOnECS {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunWriteRandomDataTaskOnECS.class);

    private final InstanceProperties instanceProperties;
    private final SystemTestPropertyValues systemTestProperties;
    private final EcsClient ecsClient;
    private final List<String> args;

    public RunWriteRandomDataTaskOnECS(SystemTestProperties systemTestProperties, EcsClient ecsClient) {
        this.instanceProperties = systemTestProperties;
        this.systemTestProperties = systemTestProperties.testPropertiesOnly();
        this.ecsClient = ecsClient;
        this.args = List.of(
                "combined",
                systemTestProperties.get(CONFIG_BUCKET),
                systemTestProperties.get(INGEST_BY_QUEUE_ROLE_ARN));
    }

    public RunWriteRandomDataTaskOnECS(
            InstanceProperties instanceProperties, SystemTestStandaloneProperties systemTestProperties,
            EcsClient ecsClient) {
        this.instanceProperties = instanceProperties;
        this.systemTestProperties = systemTestProperties;
        this.ecsClient = ecsClient;
        this.args = List.of(
                "standalone",
                systemTestProperties.get(SYSTEM_TEST_BUCKET_NAME));
    }

    public List<RunTaskResponse> runTasks(int numberOfTasks) {

        ContainerOverride containerOverride = ContainerOverride.builder()
                .name(SYSTEM_TEST_CONTAINER)
                .command(args)
                .build();
        TaskOverride override = TaskOverride.builder()
                .containerOverrides(containerOverride)
                .build();
        AwsVpcConfiguration vpcConfiguration = AwsVpcConfiguration.builder()
                .subnets(instanceProperties.getList(SUBNETS))
                .securityGroups(instanceProperties.getList(ECS_SECURITY_GROUPS))
                .build();
        NetworkConfiguration networkConfiguration = NetworkConfiguration.builder()
                .awsvpcConfiguration(vpcConfiguration)
                .build();
        RunTaskRequest runTaskRequest = RunTaskRequest.builder()
                .cluster(systemTestProperties.get(SYSTEM_TEST_CLUSTER_NAME))
                .launchType(LaunchType.FARGATE)
                .taskDefinition(systemTestProperties.get(WRITE_DATA_TASK_DEFINITION_FAMILY))
                .networkConfiguration(networkConfiguration)
                .overrides(override)
                .propagateTags(PropagateTags.TASK_DEFINITION)
                .platformVersion(instanceProperties.get(FARGATE_VERSION))
                .build();
        List<RunTaskResponse> responses = new ArrayList<>();
        RunECSTasks.runTasksOrThrow(builder -> builder
                .ecsClient(ecsClient)
                .runTaskRequest(runTaskRequest)
                .numberOfTasksToCreate(numberOfTasks)
                .responseConsumer(responses::add));
        LOGGER.debug("Ran {} tasks", numberOfTasks);
        return responses;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3) {
            System.out.println("Usage: <instance-id> <table-name> <optional-output-file>");
            return;
        }

        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create();
                SqsClient sqsClient = SqsClient.create();
                EcsClient ecsClient = EcsClient.create()) {
            SystemTestProperties systemTestProperties = SystemTestProperties.loadFromS3GivenInstanceId(s3Client, args[0]);
            TableProperties tableProperties = S3TableProperties.createProvider(systemTestProperties, s3Client, dynamoClient).getByName(args[1]);
            SystemTestDataGenerationJobSender jobSender = new SystemTestDataGenerationJobSender(systemTestProperties.testPropertiesOnly(), sqsClient);
            RunWriteRandomDataTaskOnECS runWriteRandomDataTaskOnECS = new RunWriteRandomDataTaskOnECS(systemTestProperties, ecsClient);

            jobSender.sendJobsToQueue(SystemTestDataGenerationJob.getDefaultJobs(systemTestProperties, tableProperties));
            List<RunTaskResponse> results = runWriteRandomDataTaskOnECS.runTasks(systemTestProperties.getInt(NUMBER_OF_WRITERS));
            if (args.length > 2) {
                TasksJson.writeToFile(results, Paths.get(args[2]));
            }
        }
    }
}
