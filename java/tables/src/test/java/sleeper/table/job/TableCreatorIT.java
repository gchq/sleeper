/*
 * Copyright 2022 Crown Copyright
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
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class TableCreatorIT {
    @ClassRule
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    private static final Schema KEY_VALUE_SCHEMA = new Schema();

    @ClassRule
    public static TemporaryFolder tempDir = new TemporaryFolder();

    static {
        KEY_VALUE_SCHEMA.setRowKeyFields(new Field("key", new StringType()));
        KEY_VALUE_SCHEMA.setValueFields(new Field("value", new StringType()));
    }

    private AmazonS3 s3Client;
    private AmazonDynamoDB dynamoClient;

    @Before
    public void createClients() {
        s3Client = getS3Client();
        dynamoClient = getDynamoClient();
    }

    @After
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
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    private AmazonDynamoDB getDynamoClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
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
        assertTrue(s3Client.doesBucketExistV2("sleeper-" + instanceProperties.get(ID) + "-table-test"));
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
        assertTrue(s3Client.doesBucketExistV2(expected));
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
        assertTrue(s3Client.doesBucketExistV2(expected));
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
        List<String> expectedTables = Lists.newArrayList(
                "sleeper-" + instanceId + "-table-mytable-active-files",
                "sleeper-" + instanceId + "-table-mytable-gc-files",
                "sleeper-" + instanceId + "-table-mytable-partitions");
        List<String> tableNames = dynamoClient.listTables().getTableNames();

        assertTrue(tableNames.containsAll(expectedTables));
    }

    @Test
    public void shouldNotOverwriteDataBucketIfAlreadyDefined() throws IOException {
        // Given
        InstanceProperties instanceProperties = createInstanceProperties();
        TableCreator tableCreator = new TableCreator(s3Client, dynamoClient, instanceProperties);

        // When
        String localDir = tempDir.newFolder().getAbsolutePath();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(KEY_VALUE_SCHEMA);
        tableProperties.set(TABLE_NAME, "MyTable");
        tableProperties.set(DATA_BUCKET, localDir);

        tableCreator.createTable(tableProperties);

        // Then
        assertEquals(localDir, tableProperties.get(DATA_BUCKET));
    }
}
