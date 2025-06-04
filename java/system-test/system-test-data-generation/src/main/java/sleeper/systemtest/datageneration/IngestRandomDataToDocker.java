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

package sleeper.systemtest.datageneration;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.runner.IngestFactory;
import sleeper.ingest.runner.impl.commit.AddFilesToStateStore;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestorev2.StateStoreFactory;
import sleeper.systemtest.configurationv2.SystemTestDataGenerationJob;

import java.io.IOException;
import java.net.URI;

import static sleeper.configurationv2.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class IngestRandomDataToDocker {
    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final S3Client s3;
    private final DynamoDbClient dynamoDB;
    private final long numberOfRecords;

    public IngestRandomDataToDocker(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            S3Client s3, DynamoDbClient dynamoDB, long numberOfRecords) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.s3 = s3;
        this.dynamoDB = dynamoDB;
        this.numberOfRecords = numberOfRecords;
    }

    private void run() throws IOException {
        StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3,
                dynamoDB, HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
        SystemTestDataGenerationJob job = SystemTestDataGenerationJob.builder()
                .tableName(tableProperties.get(TABLE_NAME))
                .recordsPerIngest(numberOfRecords)
                .build();
        WriteRandomDataDirect.writeWithIngestFactory(
                IngestFactory.builder()
                        .objectFactory(ObjectFactory.noUserJars())
                        .localDir("/tmp/scratch")
                        .stateStoreProvider(stateStoreProvider)
                        .s3AsyncClient(buildS3AsyncClient(S3AsyncClient.builder()))
                        .instanceProperties(instanceProperties)
                        .build(),
                AddFilesToStateStore.synchronousNoJob(stateStoreProvider.getStateStore(tableProperties)),
                job, tableProperties);
    }

    private static S3AsyncClient buildS3AsyncClient(S3AsyncClientBuilder builder) {
        URI customEndpoint = getCustomEndpoint();
        if (customEndpoint != null) {
            return builder
                    .endpointOverride(customEndpoint)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                            "test-access-key", "test-secret-key")))
                    .region(Region.US_EAST_1)
                    .forcePathStyle(true)
                    .build();
        } else {
            return builder.build();
        }
    }

    private static URI getCustomEndpoint() {
        String endpoint = System.getenv("AWS_ENDPOINT_URL");
        if (endpoint != null) {
            return URI.create(endpoint);
        }
        return null;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3) {
            throw new IllegalArgumentException("Usage: <instance-id> <table-name> <optional-number-of-records>");
        }
        String instanceId = args[0];
        String tableName = args[1];
        long numberOfRecords = 100000;
        if (args.length > 2) {
            numberOfRecords = Long.parseLong(args[2]);
        }

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableProperties tableProperties = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient)
                    .loadByName(tableName);

            new IngestRandomDataToDocker(instanceProperties, tableProperties, s3Client, dynamoClient, numberOfRecords)
                    .run();
        }
    }
}
