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

package sleeper.configuration.properties;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.local.LoadLocalProperties.loadInstanceProperties;
import static sleeper.core.properties.local.LoadLocalProperties.loadTablesFromInstancePropertiesFile;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

@Testcontainers
class SaveLocalPropertiesS3IT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    private final AmazonS3 s3Client = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoClient = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    @TempDir
    private Path tempDir;

    private InstanceProperties saveFromS3(String instanceId) throws IOException {
        return S3InstanceProperties.saveToLocalWithTableProperties(s3Client, dynamoClient, instanceId, tempDir);
    }

    @Test
    void shouldLoadInstancePropertiesFromS3() throws IOException {
        // Given
        InstanceProperties properties = createTestInstance();

        // When
        saveFromS3(properties.get(ID));

        // Then
        assertThat(loadInstanceProperties(tempDir.resolve("instance.properties")))
                .isEqualTo(properties);
    }

    @Test
    void shouldLoadTablePropertiesFromS3() throws IOException {
        // Given
        InstanceProperties properties = createTestInstance();
        TableProperties table1 = createTestTable(properties, schemaWithKey("key1"));
        TableProperties table2 = createTestTable(properties, schemaWithKey("key2"));

        // When
        saveFromS3(properties.get(ID));

        // Then
        assertThat(loadTablesFromInstancePropertiesFile(properties, tempDir.resolve("instance.properties")))
                .containsExactlyInAnyOrder(table1, table2);
    }

    @Test
    void shouldLoadNoTablePropertiesFromS3WhenNoneAreSaved() throws IOException {
        // Given
        InstanceProperties properties = createTestInstance();

        // When
        saveFromS3(properties.get(ID));

        // Then
        assertThat(loadTablesFromInstancePropertiesFile(properties, tempDir.resolve("instance.properties"))).isEmpty();
    }

    @Test
    void shouldLoadAndReturnInstancePropertiesFromS3() throws IOException {
        // Given
        InstanceProperties properties = createTestInstance();

        // When
        InstanceProperties saved = saveFromS3(properties.get(ID));

        // Then
        assertThat(saved).isEqualTo(properties);
    }

    private InstanceProperties createTestInstance() {
        InstanceProperties instanceProperties = S3InstancePropertiesTestHelper.createTestInstanceProperties(s3Client);
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        return instanceProperties;
    }

    private TableProperties createTestTable(InstanceProperties instanceProperties, Schema schema) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient).save(tableProperties);
        return tableProperties;
    }
}
