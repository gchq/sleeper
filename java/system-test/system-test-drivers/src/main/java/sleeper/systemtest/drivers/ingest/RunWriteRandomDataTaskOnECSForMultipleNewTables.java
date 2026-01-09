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

import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.table.AddTable;
import sleeper.clients.table.TakeAllTablesOffline;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.PropertiesUtils;
import sleeper.core.properties.instance.CommonProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.statestore.StateStoreFactory;
import sleeper.systemtest.configuration.SystemTestDataGenerationJob;
import sleeper.systemtest.configuration.SystemTestProperties;
import sleeper.systemtest.configuration.SystemTestProperty;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

public class RunWriteRandomDataTaskOnECSForMultipleNewTables {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunWriteRandomDataTaskOnECSForMultipleNewTables.class);

    private S3Client s3Client;
    private DynamoDbClient dynamoClient;
    private EcsClient ecsClient;

    public RunWriteRandomDataTaskOnECSForMultipleNewTables(S3Client s3Client, DynamoDbClient dynamoClient, EcsClient ecsClient) {
        this.s3Client = s3Client;
        this.dynamoClient = dynamoClient;
        this.ecsClient = ecsClient;
    }

    public void takeAllTablesOffline(InstanceProperties instanceProperties) {
        TakeAllTablesOffline offliner = new TakeAllTablesOffline(s3Client, dynamoClient);
        offliner.takeAllOffline(instanceProperties);
    }

    public List<String> createTables(InstanceProperties instanceProperties, int tableCount, Path tablePropertiesFile, Path schemaFile, String splitPointsFile) throws IOException {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyMMdd-HHmm");
        String tablePrefix = "table-" + Instant.now().toString(formatter) + "-";

        List<String> tableNames = IntStream.rangeClosed(1, tableCount).mapToObj(i -> {
            String tableName = tablePrefix + i;

            try {
                TableProperties tableProperties = new TableProperties(instanceProperties, PropertiesUtils.loadProperties(tablePropertiesFile));
                tableProperties.set(TableProperty.TABLE_NAME, tableName);
                tableProperties.setSchema(Schema.load(schemaFile));
                tableProperties.set(TableProperty.SPLIT_POINTS_FILE, splitPointsFile);

                TablePropertiesStore tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);
                StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient);
                new AddTable(instanceProperties, tableProperties, tablePropertiesStore, stateStoreProvider).run();
                LOGGER.info("Added table " + instanceProperties.get(CommonProperty.ID) + ":" + tableName);
            } catch (Exception e) {
                throw new RuntimeException("Unable to create table " + instanceProperties.get(CommonProperty.ID) + ":" + tableName, e);
            }

            return tableName;
        }).collect(Collectors.toList());

        return tableNames;
    }

    public void run(
        SystemTestProperties systemTestProperties, int tableCount,
        Path tablePropertiesFile, Path schemaFile, String splitPointsFile,
        int numWritersPerTable, SystemTestDataGenerationJob.Builder jobSpec
    ) throws IOException {

        LOGGER.info("Taking tables offline");
        takeAllTablesOffline(systemTestProperties);

        LOGGER.info("Creating " + tableCount + " tables");
        createTables(systemTestProperties, tableCount, tablePropertiesFile, schemaFile, splitPointsFile);

        LOGGER.info("Submitting ingest tasks");
        new RunWriteRandomDataTaskOnECSForAllOnlineTables(s3Client, dynamoClient, ecsClient)
            .run(systemTestProperties, jobSpec, numWritersPerTable);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 5) {
            System.out.println("Usage: <instanceId> <tableCount> <tablePropertiesFile> <schemaFile> <splitPointsFile> [num-writers-per-table] [num-ingests-per-writer] [records-per-ingest]");
            System.exit(1);
        }

        String instanceId = args[0];
        int tableCount = Integer.parseInt(args[1]);
        Path tablePropertiesFile = Path.of(args[2]);
        Path schemaFile = Path.of(args[3]);
        String splitPointsFile = args[4];

        try (
            S3Client s3Client = buildAwsV2Client(S3Client.builder());
            DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
            EcsClient ecsClient = buildAwsV2Client(EcsClient.builder());
        ) {
            SystemTestProperties systemTestProperties = SystemTestProperties.loadFromS3GivenInstanceId(s3Client, instanceId);

            int numWritersPerTable = args.length > 5 ? Integer.parseInt(args[5]) : systemTestProperties.getInt(SystemTestProperty.NUMBER_OF_WRITERS);

            SystemTestDataGenerationJob.Builder jobSpec = SystemTestDataGenerationJob.builder()
                .instanceProperties(systemTestProperties)
                .testProperties(systemTestProperties.testPropertiesOnly());

            if (args.length > 6) {
                jobSpec.numberOfIngests(Integer.parseInt(args[6]));
            }

            if (args.length > 7) {
                jobSpec.rowsPerIngest(Long.parseLong(args[7]));
            }

            new RunWriteRandomDataTaskOnECSForMultipleNewTables(s3Client, dynamoClient, ecsClient).run(
                systemTestProperties, tableCount,
                tablePropertiesFile, schemaFile, splitPointsFile,
                numWritersPerTable, jobSpec
            );
        }
    }

}
