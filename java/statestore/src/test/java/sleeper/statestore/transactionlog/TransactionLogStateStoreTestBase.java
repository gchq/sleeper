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
package sleeper.statestore.transactionlog;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.statestore.StateStore;

import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class TransactionLogStateStoreTestBase {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);
    protected static AmazonDynamoDB dynamoDBClient;
    protected static AmazonS3 s3Client;
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final Configuration configuration = new Configuration();

    @BeforeAll
    public static void initDynamoClient() {
        dynamoDBClient = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
        s3Client = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    }

    @AfterAll
    public static void shutdownDynamoClient() {
        dynamoDBClient.shutdown();
    }

    @BeforeEach
    void setUpBase() {
        new TransactionLogStateStoreCreator(instanceProperties, dynamoDBClient, s3Client).create();
    }

    public StateStore createStateStore(TableProperties tableProperties) {
        return new DynamoDBTransactionLogStateStore(instanceProperties, tableProperties, dynamoDBClient, s3Client, configuration);
    }
}
