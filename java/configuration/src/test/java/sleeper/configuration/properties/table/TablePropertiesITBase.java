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
package sleeper.configuration.properties.table;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

/**
 * A base class for tests storing table properties in S3 with a DynamoDB table index, in LocalStack.
 */
@Testcontainers
public abstract class TablePropertiesITBase {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    private static final Schema KEY_VALUE_SCHEMA = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .valueFields(new Field("value", new StringType()))
            .build();

    protected final AmazonS3 s3Client = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    protected final AmazonDynamoDB dynamoDBClient = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());

    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final TableProperties tableProperties = createValidTableProperties();
    protected final TablePropertiesStore store = S3TableProperties.createStore(instanceProperties, s3Client, dynamoDBClient);
    protected final String tableName = tableProperties.get(TABLE_NAME);
    protected final String tableId = tableProperties.get(TABLE_ID);

    @BeforeEach
    void setUp() {
        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));
        DynamoDBTableIndexCreator.create(dynamoDBClient, instanceProperties);
    }

    protected TableProperties createValidTableProperties() {
        return createTestTableProperties(instanceProperties, KEY_VALUE_SCHEMA);
    }
}
