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
package sleeper.systemtest.ingest;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.systemtest.SystemTestProperties;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FARGATE_VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;
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

    public RunWriteRandomDataTaskOnECS(SystemTestProperties systemTestProperties, TableProperties tableProperties) {
        this.systemTestProperties = systemTestProperties;
        this.tableProperties = tableProperties;
    }

    public void run() throws InterruptedException {
        AmazonECS ecsClient = AmazonECSClientBuilder.defaultClient();

        List<String> args = Arrays.asList(
                systemTestProperties.get(CONFIG_BUCKET),
                tableProperties.get(TABLE_NAME));

        ContainerOverride containerOverride = new ContainerOverride()
                .withName(SYSTEM_TEST_CONTAINER)
                .withCommand(args);

        TaskOverride override = new TaskOverride()
                .withContainerOverrides(containerOverride);

        AwsVpcConfiguration vpcConfiguration = new AwsVpcConfiguration()
                .withSubnets(systemTestProperties.get(SUBNET));

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

        for (int i = 0; i < systemTestProperties.getInt(NUMBER_OF_WRITERS); i++) {
            if (i > 0 && i % 10 == 0) {
                Thread.sleep(10 * 1000L);
                LOGGER.debug("10 tasks submitted, sleeping for 10 seconds");
            }
            ecsClient.runTask(runTaskRequest);
            // Sleep for 10 seconds - API allows 1 job per second with a burst of 10 jobs in a second
        }
        LOGGER.debug("Ran {} tasks", systemTestProperties.getInt(NUMBER_OF_WRITERS));

        ecsClient.shutdown();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (2 != args.length) {
            System.out.println("Usage: <instance id> <table name>");
            return;
        }

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.loadFromS3GivenInstanceId(s3Client, args[0]);
        TableProperties tableProperties = new TablePropertiesProvider(s3Client, systemTestProperties).getTableProperties(args[1]);
        RunWriteRandomDataTaskOnECS runWriteRandomDataTaskOnECS = new RunWriteRandomDataTaskOnECS(systemTestProperties, tableProperties);
        runWriteRandomDataTaskOnECS.run();

        s3Client.shutdown();
    }
}
