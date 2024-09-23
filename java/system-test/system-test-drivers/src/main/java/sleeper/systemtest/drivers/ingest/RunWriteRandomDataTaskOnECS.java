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
package sleeper.systemtest.drivers.ingest;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.ecs.model.AwsVpcConfiguration;
import com.amazonaws.services.ecs.model.ContainerOverride;
import com.amazonaws.services.ecs.model.LaunchType;
import com.amazonaws.services.ecs.model.NetworkConfiguration;
import com.amazonaws.services.ecs.model.PropagateTags;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.RunTaskResult;
import com.amazonaws.services.ecs.model.TaskOverride;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.systemtest.configuration.SystemTestProperties;
import sleeper.systemtest.configuration.SystemTestPropertyValues;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;
import sleeper.systemtest.drivers.ingest.json.TasksJson;
import sleeper.task.common.RunECSTasks;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_BY_QUEUE_ROLE_ARN;
import static sleeper.configuration.properties.instance.CommonProperty.ECS_SECURITY_GROUPS;
import static sleeper.configuration.properties.instance.CommonProperty.FARGATE_VERSION;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
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
    private final AmazonECS ecsClient;
    private final List<String> args;

    public RunWriteRandomDataTaskOnECS(SystemTestProperties systemTestProperties, TableProperties tableProperties, AmazonECS ecsClient) {
        this.instanceProperties = systemTestProperties;
        this.systemTestProperties = systemTestProperties.testPropertiesOnly();
        this.ecsClient = ecsClient;
        this.args = List.of(
                instanceProperties.get(CONFIG_BUCKET),
                tableProperties.get(TABLE_NAME),
                instanceProperties.get(INGEST_BY_QUEUE_ROLE_ARN));
    }

    public RunWriteRandomDataTaskOnECS(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            SystemTestStandaloneProperties systemTestProperties, AmazonECS ecsClient) {
        this.instanceProperties = instanceProperties;
        this.systemTestProperties = systemTestProperties;
        this.ecsClient = ecsClient;
        this.args = List.of(
                instanceProperties.get(CONFIG_BUCKET),
                tableProperties.get(TABLE_NAME),
                instanceProperties.get(INGEST_BY_QUEUE_ROLE_ARN),
                systemTestProperties.get(SYSTEM_TEST_BUCKET_NAME));
    }

    public List<RunTaskResult> run() {

        ContainerOverride containerOverride = new ContainerOverride()
                .withName(SYSTEM_TEST_CONTAINER)
                .withCommand(args);

        TaskOverride override = new TaskOverride()
                .withContainerOverrides(containerOverride);

        AwsVpcConfiguration vpcConfiguration = new AwsVpcConfiguration()
                .withSubnets(instanceProperties.getList(SUBNETS))
                .withSecurityGroups(instanceProperties.getList(ECS_SECURITY_GROUPS));

        NetworkConfiguration networkConfiguration = new NetworkConfiguration()
                .withAwsvpcConfiguration(vpcConfiguration);

        RunTaskRequest runTaskRequest = new RunTaskRequest()
                .withCluster(systemTestProperties.get(SYSTEM_TEST_CLUSTER_NAME))
                .withLaunchType(LaunchType.FARGATE)
                .withTaskDefinition(systemTestProperties.get(WRITE_DATA_TASK_DEFINITION_FAMILY))
                .withNetworkConfiguration(networkConfiguration)
                .withOverrides(override)
                .withPropagateTags(PropagateTags.TASK_DEFINITION)
                .withPlatformVersion(instanceProperties.get(FARGATE_VERSION));

        List<RunTaskResult> results = new ArrayList<>();
        RunECSTasks.runTasksOrThrow(builder -> builder
                .ecsClient(ecsClient)
                .runTaskRequest(runTaskRequest)
                .numberOfTasksToCreate(systemTestProperties.getInt(NUMBER_OF_WRITERS))
                .resultConsumer(results::add));
        LOGGER.debug("Ran {} tasks", systemTestProperties.getInt(NUMBER_OF_WRITERS));
        return results;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3) {
            System.out.println("Usage: <instance-id> <table-name> <optional-output-file>");
            return;
        }

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
        AmazonECS ecsClient = AmazonECSClientBuilder.defaultClient();
        try {
            SystemTestProperties systemTestProperties = SystemTestProperties.loadFromS3GivenInstanceId(s3Client, args[0]);
            TableProperties tableProperties = new TablePropertiesProvider(systemTestProperties, s3Client, dynamoClient).getByName(args[1]);
            RunWriteRandomDataTaskOnECS runWriteRandomDataTaskOnECS = new RunWriteRandomDataTaskOnECS(systemTestProperties, tableProperties, ecsClient);
            List<RunTaskResult> results = runWriteRandomDataTaskOnECS.run();
            if (args.length > 2) {
                TasksJson.writeToFile(results, Paths.get(args[2]));
            }
        } finally {
            s3Client.shutdown();
            dynamoClient.shutdown();
            ecsClient.shutdown();
        }
    }
}
