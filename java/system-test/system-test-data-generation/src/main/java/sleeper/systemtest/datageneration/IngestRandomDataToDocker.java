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

package sleeper.systemtest.datageneration;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.ingest.runner.IngestFactory;
import sleeper.ingest.runner.impl.commit.AddFilesToStateStore;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreFactory;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.io.IOException;
import java.net.URI;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_INGEST;

public class IngestRandomDataToDocker {
    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final AmazonS3 s3;
    private final AmazonDynamoDB dynamoDB;
    private final long numberOfRecords;

    public IngestRandomDataToDocker(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            AmazonS3 s3, AmazonDynamoDB dynamoDB, long numberOfRecords) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.s3 = s3;
        this.dynamoDB = dynamoDB;
        this.numberOfRecords = numberOfRecords;
    }

    private void run() throws IOException {
        SystemTestStandaloneProperties systemTestProperties = new SystemTestStandaloneProperties();
        systemTestProperties.setNumber(NUMBER_OF_RECORDS_PER_INGEST, numberOfRecords);
        StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3,
                dynamoDB, HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
        WriteRandomDataDirect.writeWithIngestFactory(
                IngestFactory.builder()
                        .objectFactory(ObjectFactory.noUserJars())
                        .localDir("/tmp/scratch")
                        .stateStoreProvider(stateStoreProvider)
                        .s3AsyncClient(buildS3AsyncClient(S3AsyncClient.builder()))
                        .instanceProperties(instanceProperties)
                        .build(),
                AddFilesToStateStore.synchronous(stateStoreProvider.getStateStore(tableProperties)),
                systemTestProperties, tableProperties);
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

        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        try {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableProperties tableProperties = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient)
                    .loadByName(tableName);

            new IngestRandomDataToDocker(instanceProperties, tableProperties, s3Client, dynamoClient, numberOfRecords)
                    .run();
        } finally {
            s3Client.shutdown();
            dynamoClient.shutdown();
        }
    }
}
