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
import software.amazon.awssdk.services.ecs.model.RunTaskResponse;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TableProperty;
import sleeper.systemtest.configuration.SystemTestDataGenerationJob;
import sleeper.systemtest.configuration.SystemTestProperties;
import sleeper.systemtest.configuration.SystemTestProperty;
import sleeper.systemtest.drivers.ingest.json.TasksJson;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

public class RunWriteRandomDataTaskOnECSForAllOnlineTables {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunWriteRandomDataTaskOnECSForAllOnlineTables.class);

    private S3Client s3Client;
    private DynamoDbClient dynamoClient;
    private EcsClient ecsClient;

    public RunWriteRandomDataTaskOnECSForAllOnlineTables(S3Client s3Client, DynamoDbClient dynamoClient, EcsClient ecsClient) {
        this.s3Client = s3Client;
        this.dynamoClient = dynamoClient;
        this.ecsClient = ecsClient;
    }

    public List<RunTaskResponse> run(SystemTestProperties systemTestProperties, SystemTestDataGenerationJob.Builder jobSpec, int numWritersPerTable) {
        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(systemTestProperties, s3Client, dynamoClient);
        RunWriteRandomDataTaskOnECS runner = new RunWriteRandomDataTaskOnECS(systemTestProperties, ecsClient, s3Client);

        List<RunTaskResponse> responses = tablePropertiesProvider.streamOnlineTables()
            .flatMap(tableProperties -> {
                String tableName = tableProperties.get(TableProperty.TABLE_NAME);
                LOGGER.info("Submitting an ingest job with {} writers for table {}", numWritersPerTable, tableName);
                SystemTestDataGenerationJob job = jobSpec
                    .tableName(tableName)
                    .build();
                List<RunTaskResponse> response = runner.runTasks(numWritersPerTable, job);
                return response.stream();
            })
            .toList();

        return responses;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1 || args.length > 5) {
            System.out.println("Usage: <instance-id> [num-writers-per-table] [num-ingests-per-writer] [records-per-ingest] [output-file]");
            return;
        }

        String instanceId = args[0];

        try (
            S3Client s3Client = S3Client.create();
            DynamoDbClient dynamoClient = DynamoDbClient.create();
            EcsClient ecsClient = EcsClient.create();
        ) {
            SystemTestProperties systemTestProperties = SystemTestProperties.loadFromS3GivenInstanceId(s3Client, instanceId);

            int numWritersPerTable = args.length > 1 ? Integer.parseInt(args[1]) : systemTestProperties.getInt(SystemTestProperty.NUMBER_OF_WRITERS);

            SystemTestDataGenerationJob.Builder jobSpec = SystemTestDataGenerationJob.builder()
                .instanceProperties(systemTestProperties)
                .testProperties(systemTestProperties.testPropertiesOnly());

            if (args.length > 2) {
                jobSpec.numberOfIngests(Integer.parseInt(args[2]));
            }

            if (args.length > 3) {
                jobSpec.rowsPerIngest(Long.parseLong(args[3]));
            }

            RunWriteRandomDataTaskOnECSForAllOnlineTables runner = new RunWriteRandomDataTaskOnECSForAllOnlineTables(s3Client, dynamoClient, ecsClient);
            List<RunTaskResponse> responses = runner.run(systemTestProperties, jobSpec, numWritersPerTable);

            if (args.length > 4) {
                TasksJson.writeToFile(responses, Paths.get(args[4]));
            }
        }
    }
}
