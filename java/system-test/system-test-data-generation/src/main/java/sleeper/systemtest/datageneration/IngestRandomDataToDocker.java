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
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.ingest.IngestFactory;
import sleeper.statestore.StateStoreProvider;
import sleeper.systemtest.configuration.SystemTestProperties;
import sleeper.utils.HadoopConfigurationProvider;

import java.io.IOException;
import java.net.URI;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_WRITER;

public class IngestRandomDataToDocker {
    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final AmazonDynamoDB dynamoDB;
    private final long numberOfRecords;

    public IngestRandomDataToDocker(InstanceProperties instanceProperties, TableProperties tableProperties,
                                    AmazonDynamoDB dynamoDB, long numberOfRecords) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.dynamoDB = dynamoDB;
        this.numberOfRecords = numberOfRecords;
    }

    private void run() throws IOException {
        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.setNumber(NUMBER_OF_RECORDS_PER_WRITER, numberOfRecords);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDB,
                instanceProperties, HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
        WriteRandomDataDirect.writeWithIngestFactory(
                IngestFactory.builder()
                        .objectFactory(ObjectFactory.noUserJars())
                        .localDir("/tmp/scratch")
                        .stateStoreProvider(stateStoreProvider)
                        .s3AsyncClient(buildS3AsyncClient(S3AsyncClient.builder()))
                        .instanceProperties(instanceProperties)
                        .build(),
                systemTestProperties.testPropertiesOnly(), tableProperties);
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

    public static void main(String[] args) throws IOException, ObjectFactoryException {
        if (args.length < 1 || args.length > 2) {
            throw new IllegalArgumentException("Usage: <instance-id> <optional-number-of-records>");
        }
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDB = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3Client, args[0]);
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.loadFromS3(s3Client, "system-test");
        long numberOfRecords = 100000;
        if (args.length > 1) {
            numberOfRecords = Long.parseLong(args[1]);
        }

        new IngestRandomDataToDocker(instanceProperties, tableProperties, dynamoDB, numberOfRecords)
                .run();
    }
}
