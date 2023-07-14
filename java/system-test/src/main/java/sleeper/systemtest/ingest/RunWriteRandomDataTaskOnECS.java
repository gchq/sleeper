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
package sleeper.systemtest.ingest;

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

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.job.common.RunECSTasks;
import sleeper.systemtest.SystemTestProperties;
import sleeper.systemtest.ingest.json.TasksJson;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static sleeper.configuration.properties.instance.CommonProperty.FARGATE_VERSION;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.systemtest.SystemTestProperty.NUMBER_OF_WRITERS;
import static sleeper.systemtest.SystemTestProperty.SYSTEM_TEST_CLUSTER_NAME;
import static sleeper.systemtest.SystemTestProperty.WRITE_DATA_TASK_DEFINITION_FAMILY;
import static sleeper.systemtest.cdk.SystemTestStack.SYSTEM_TEST_CONTAINER;

/**
 * Runs ECS tasks to write random data.
 */
public class RunWriteRandomDataTaskOnECS {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunWriteRandomDataTaskOnECS.class);

    private final SystemTestProperties systemTestProperties;
    private final TableProperties tableProperties;
    private final AmazonECS ecsClient;

    public RunWriteRandomDataTaskOnECS(SystemTestProperties systemTestProperties, TableProperties tableProperties, AmazonECS ecsClient) {
        this.systemTestProperties = systemTestProperties;
        this.tableProperties = tableProperties;
        this.ecsClient = ecsClient;
    }

    public List<RunTaskResult> run() {

        List<String> args = Arrays.asList(
                systemTestProperties.get(CONFIG_BUCKET),
                tableProperties.get(TABLE_NAME));

        ContainerOverride containerOverride = new ContainerOverride()
                .withName(SYSTEM_TEST_CONTAINER)
                .withCommand(args);

        TaskOverride override = new TaskOverride()
                .withContainerOverrides(containerOverride);

        AwsVpcConfiguration vpcConfiguration = new AwsVpcConfiguration()
                .withSubnets(systemTestProperties.getList(SUBNETS));

        NetworkConfiguration networkConfiguration = new NetworkConfiguration()
                .withAwsvpcConfiguration(vpcConfiguration);

        RunTaskRequest runTaskRequest = new RunTaskRequest()
                .withCluster(systemTestProperties.get(SYSTEM_TEST_CLUSTER_NAME))
                .withLaunchType(LaunchType.FARGATE)
                .withTaskDefinition(systemTestProperties.get(WRITE_DATA_TASK_DEFINITION_FAMILY))
                .withNetworkConfiguration(networkConfiguration)
                .withOverrides(override)
                .withPropagateTags(PropagateTags.TASK_DEFINITION)
                .withPlatformVersion(systemTestProperties.get(FARGATE_VERSION));

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
            System.out.println("Usage: <instance id> <table name> <optional_output_file>");
            return;
        }

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonECS ecsClient = AmazonECSClientBuilder.defaultClient();
        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.loadFromS3GivenInstanceId(s3Client, args[0]);
        TableProperties tableProperties = new TablePropertiesProvider(s3Client, systemTestProperties).getTableProperties(args[1]);
        RunWriteRandomDataTaskOnECS runWriteRandomDataTaskOnECS = new RunWriteRandomDataTaskOnECS(systemTestProperties, tableProperties, ecsClient);
        List<RunTaskResult> results = runWriteRandomDataTaskOnECS.run();
        if (args.length > 2) {
            TasksJson.writeToFile(results, Paths.get(args[2]));
        }

        s3Client.shutdown();
        ecsClient.shutdown();
    }
}
