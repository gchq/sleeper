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
package sleeper.table.job;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class TableCreatorIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    private static final Schema KEY_VALUE_SCHEMA = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .valueFields(new Field("value", new StringType()))
            .build();

    @TempDir
    public static Path tempDir;

    private AmazonS3 s3Client;
    private AmazonDynamoDB dynamoClient;

    @BeforeEach
    public void createClients() {
        s3Client = getS3Client();
        dynamoClient = getDynamoClient();
    }

    @AfterEach
    public void shutDownClients() {
        s3Client.shutdown();
        dynamoClient.shutdown();
    }

    private InstanceProperties createInstanceProperties() {
        String instanceId = UUID.randomUUID().toString();
        String configBucket = "sleeper-" + instanceId + "-config";

        s3Client.createBucket(configBucket);
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, instanceId);
        instanceProperties.set(CONFIG_BUCKET, configBucket);
        return instanceProperties;
    }

    private AmazonS3 getS3Client() {
        return buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    }

    private AmazonDynamoDB getDynamoClient() {
        return buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    }

    @Test
    public void shouldCreateS3Bucket() {
        // Given
        InstanceProperties instanceProperties = createInstanceProperties();

        TableCreator tableCreator = new TableCreator(s3Client, dynamoClient, instanceProperties);

        // When
        String tableName = "test";
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(KEY_VALUE_SCHEMA);
        tableProperties.set(TABLE_NAME, tableName);

        tableCreator.createTable(tableProperties);

        // Then
        assertThat(s3Client.doesBucketExistV2("sleeper-" + instanceProperties.get(ID) + "-table-test")).isTrue();
    }

    @Test
    public void shouldTruncateS3BucketNameTo63Characters() {
        // Given
        String alphabet = "abcdefghijklmnopqrstuvwxyz";
        String configBucket = "sleeper-" + alphabet + "-config";
        s3Client.createBucket(configBucket);

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, alphabet);
        instanceProperties.set(CONFIG_BUCKET, configBucket);

        TableCreator tableCreator = new TableCreator(s3Client, dynamoClient, instanceProperties);

        // When
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(KEY_VALUE_SCHEMA);
        tableProperties.set(TABLE_NAME, alphabet + alphabet);

        tableCreator.createTable(tableProperties);

        // Then
        // one alphabet (26 characters)
        // + plus "sleeper" "table" and three "-" characters (15 characters)
        // + the alphabet up to v (22 characters) = 63 characters
        String expected = "sleeper-" + alphabet + "-table-" + "abcdefghijklmnopqrstuv";
        assertThat(s3Client.doesBucketExistV2(expected)).isTrue();
    }

    @Test
    public void shouldLowercaseIdAndTableNameForBucketName() {
        // Given
        String instanceId = "MySleeperInstance";
        String configBucket = ("sleeper-" + instanceId + "-config").toLowerCase();
        s3Client.createBucket(configBucket);

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, instanceId);
        instanceProperties.set(CONFIG_BUCKET, configBucket);

        TableCreator tableCreator = new TableCreator(s3Client, dynamoClient, instanceProperties);

        // When
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(KEY_VALUE_SCHEMA);
        tableProperties.set(TABLE_NAME, "MyTable");

        tableCreator.createTable(tableProperties);

        // Then
        String expected = "sleeper-mysleeperinstance-table-mytable";
        assertThat(s3Client.doesBucketExistV2(expected)).isTrue();
    }

    @Test
    public void shouldCreateDynamoDBTables() {
        // Given
        InstanceProperties instanceProperties = createInstanceProperties();

        TableCreator tableCreator = new TableCreator(s3Client, dynamoClient, instanceProperties);

        // When
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(KEY_VALUE_SCHEMA);
        tableProperties.set(TABLE_NAME, "MyTable");

        tableCreator.createTable(tableProperties);

        // Then
        String instanceId = instanceProperties.get(ID);
        assertThat(dynamoClient.listTables().getTableNames()).contains(
                "sleeper-" + instanceId + "-table-mytable-file-in-partition",
                "sleeper-" + instanceId + "-table-mytable-file-lifecycle",
                "sleeper-" + instanceId + "-table-mytable-partitions");
    }

    @Test
    public void shouldNotOverwriteDataBucketIfAlreadyDefined() throws IOException {
        // Given
        InstanceProperties instanceProperties = createInstanceProperties();
        TableCreator tableCreator = new TableCreator(s3Client, dynamoClient, instanceProperties);

        // When
        String localDir = createTempDirectory(tempDir, null).toString();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(KEY_VALUE_SCHEMA);
        tableProperties.set(TABLE_NAME, "MyTable");
        tableProperties.set(DATA_BUCKET, localDir);

        tableCreator.createTable(tableProperties);

        // Then
        assertThat(tableProperties.get(DATA_BUCKET)).isEqualTo(localDir);
    }
}
