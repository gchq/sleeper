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

package sleeper.clients.compaction;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.compaction.core.job.creation.CreateCompactionJobs;
import sleeper.compaction.job.creation.AwsCreateCompactionJobs;
import sleeper.configurationv2.jars.S3UserJarsLoader;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.util.ObjectFactoryException;
import sleeper.statestorev2.StateStoreFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configurationv2.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Command line client to create compaction jobs to be run for specified Sleeper tables.
 */
public class CreateCompactionJobsClient {
    private CreateCompactionJobsClient() {
    }

    private static final Map<String, CreateJobsMode> ARG_TO_MODE = Map.of(
            "default", CreateCompactionJobs::createJobsWithStrategy,
            "all", CreateCompactionJobs::createJobWithForceAllFiles);

    @FunctionalInterface
    public interface CreateJobsMode {
        void createJobs(CreateCompactionJobs creator, TableProperties table) throws ObjectFactoryException, IOException;
    }

    public static void main(String[] args) throws ObjectFactoryException, IOException {
        if (args.length < 2) {
            System.out.println("Usage: <mode-all-or-default> <instance-id> <table-names-as-args>");
            return;
        }
        CreateJobsMode mode = ARG_TO_MODE.get(args[0].toLowerCase(Locale.ROOT));
        if (mode == null) {
            System.out.println("Supported modes for job creation are ALL or DEFAULT");
            return;
        }
        String instanceId = args[1];
        List<String> tableNames = Stream.of(args).skip(2).toList();
        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                SqsClient sqsClient = buildAwsV2Client(SqsClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
            List<TableProperties> tables = tableNames.stream()
                    .map(name -> tablePropertiesProvider.getByName(name))
                    .collect(toUnmodifiableList());
            StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient);
            CreateCompactionJobs jobCreator = AwsCreateCompactionJobs.from(
                    new S3UserJarsLoader(instanceProperties, s3Client, Path.of("/tmp")).buildObjectFactory(),
                    instanceProperties, tablePropertiesProvider, stateStoreProvider, s3Client, sqsClient);
            for (TableProperties table : tables) {
                mode.createJobs(jobCreator, table);
            }
        }
    }
}
