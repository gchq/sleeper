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
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.CommonProperties.ID;
import static sleeper.configuration.properties.table.TableProperty.SPLIT_POINTS_BASE64_ENCODED;
import static sleeper.configuration.properties.table.TableProperty.SPLIT_POINTS_KEY;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

@Testcontainers
public class TableInitialiserIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

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

    private Schema schemaWithKeyValueTypes(PrimitiveType rowKeyType, Type valueType) {
        return Schema.builder()
                .rowKeyFields(new Field("key", rowKeyType))
                .valueFields(new Field("value", valueType))
                .build();
    }

    @Test
    public void shouldInitialiseStateStoreWithNoSplitPoints() throws IOException {
        // Given
        AmazonS3 s3Client = getS3Client();
        AmazonDynamoDB dynamoClient = getDynamoClient();

        String instanceId = UUID.randomUUID().toString();
        String configBucket = ("sleeper-" + instanceId + "-config").toLowerCase();
        s3Client.createBucket(configBucket);

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, instanceId);
        instanceProperties.set(CONFIG_BUCKET, configBucket);

        TableCreator tableCreator = new TableCreator(s3Client, dynamoClient, instanceProperties);
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schemaWithKeyValueTypes(new StringType(), new StringType()));
        tableProperties.set(TABLE_NAME, "MyTable");

        tableCreator.createTable(tableProperties);

        // When
        new TableInitialiser(s3Client, dynamoClient).initialise(instanceProperties, tableProperties, configBucket, new Configuration());

        // Then
        assertThat(dynamoClient.scan(
                new ScanRequest().withTableName("sleeper-" + instanceId + "-table-mytable-partitions")).getCount())
                .isEqualTo(Integer.valueOf(1));
    }

    @Test
    public void shouldInitialiseTableWithStringSplitPoints() throws IOException, StateStoreException {
        // Given
        AmazonS3 s3Client = getS3Client();
        AmazonDynamoDB dynamoClient = getDynamoClient();

        String instanceId = UUID.randomUUID().toString();
        String configBucket = ("sleeper-" + instanceId + "-config").toLowerCase();
        String tableName = "MyTable";
        s3Client.createBucket(configBucket);
        s3Client.putObject(configBucket, "splits/" + tableName, "a\nb\nc");

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, instanceId);
        instanceProperties.set(CONFIG_BUCKET, configBucket);

        TableCreator tableCreator = new TableCreator(s3Client, dynamoClient, instanceProperties);
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schemaWithKeyValueTypes(new StringType(), new StringType()));
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(SPLIT_POINTS_KEY, "splits/" + tableName);

        tableCreator.createTable(tableProperties);

        // When
        new TableInitialiser(s3Client, dynamoClient).initialise(instanceProperties, tableProperties, configBucket, new Configuration());

        // Then
        assertThat(dynamoClient.scan(
                new ScanRequest().withTableName("sleeper-" + instanceId + "-table-mytable-partitions")).getCount())
                .isEqualTo(Integer.valueOf(7));
        validateSplits(tableProperties, "key", "", dynamoClient, "a", "b", "c");
    }

    @Test
    public void shouldInitialiseWithBase64EncodedStringSplitPoints() throws IOException, StateStoreException {
        // Given
        AmazonS3 s3Client = getS3Client();
        AmazonDynamoDB dynamoClient = getDynamoClient();

        String instanceId = UUID.randomUUID().toString();
        String configBucket = ("sleeper-" + instanceId + "-config").toLowerCase();
        String tableName = "MyTable";
        s3Client.createBucket(configBucket);
        String content = String.join("\n", new String(Base64.encodeBase64("a".getBytes())),
                new String(Base64.encodeBase64("b".getBytes())),
                new String(Base64.encodeBase64("c".getBytes())));
        s3Client.putObject(configBucket, "splits/" + tableName, content);

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, instanceId);
        instanceProperties.set(CONFIG_BUCKET, configBucket);

        TableCreator tableCreator = new TableCreator(s3Client, dynamoClient, instanceProperties);
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schemaWithKeyValueTypes(new StringType(), new StringType()));
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(SPLIT_POINTS_BASE64_ENCODED, "true");
        tableProperties.set(SPLIT_POINTS_KEY, "splits/" + tableName);

        tableCreator.createTable(tableProperties);

        // When
        new TableInitialiser(s3Client, dynamoClient).initialise(instanceProperties, tableProperties, configBucket, new Configuration());

        // Then
        assertThat(dynamoClient.scan(
                new ScanRequest().withTableName("sleeper-" + instanceId + "-table-mytable-partitions")).getCount())
                .isEqualTo(Integer.valueOf(7));
        validateSplits(tableProperties, "key", "", dynamoClient, "a", "b", "c");
    }

    @Test
    public void shouldInitialiseTableWithLongSplitPoints() throws IOException, StateStoreException {
        // Given
        AmazonS3 s3Client = getS3Client();
        AmazonDynamoDB dynamoClient = getDynamoClient();

        String instanceId = UUID.randomUUID().toString();
        String configBucket = ("sleeper-" + instanceId + "-config").toLowerCase();
        String tableName = "MyTable";
        s3Client.createBucket(configBucket);
        s3Client.putObject(configBucket, "splits/" + tableName, "1000\n1000000\n1000000000");

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, instanceId);
        instanceProperties.set(CONFIG_BUCKET, configBucket);

        TableCreator tableCreator = new TableCreator(s3Client, dynamoClient, instanceProperties);
        TableProperties tableProperties = new TableProperties(instanceProperties);
        Schema schema = schemaWithKeyValueTypes(new LongType(), new StringType());
        tableProperties.setSchema(schema);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(SPLIT_POINTS_KEY, "splits/" + tableName);

        tableCreator.createTable(tableProperties);

        // When
        new TableInitialiser(s3Client, dynamoClient).initialise(instanceProperties, tableProperties, configBucket, new Configuration());

        // Then
        assertThat(dynamoClient.scan(
                new ScanRequest().withTableName("sleeper-" + instanceId + "-table-mytable-partitions")).getCount())
                .isEqualTo(Integer.valueOf(7));
        validateSplits(tableProperties, "key", Long.MIN_VALUE, dynamoClient, 1000L, 1_000_000L, 1_000_000_000L);
    }

    @Test
    public void shouldInitialiseTableWithIntegerSplitPoints() throws IOException, StateStoreException {
        // Given
        AmazonS3 s3Client = getS3Client();
        AmazonDynamoDB dynamoClient = getDynamoClient();

        String instanceId = UUID.randomUUID().toString();
        String configBucket = ("sleeper-" + instanceId + "-config").toLowerCase();
        String tableName = "MyTable";
        s3Client.createBucket(configBucket);
        s3Client.putObject(configBucket, "splits/" + tableName, "100\n1000\n10000");

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, instanceId);
        instanceProperties.set(CONFIG_BUCKET, configBucket);

        TableCreator tableCreator = new TableCreator(s3Client, dynamoClient, instanceProperties);
        TableProperties tableProperties = new TableProperties(instanceProperties);
        Schema schema = schemaWithKeyValueTypes(new IntType(), new StringType());
        tableProperties.setSchema(schema);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(SPLIT_POINTS_KEY, "splits/" + tableName);

        tableCreator.createTable(tableProperties);

        // When
        new TableInitialiser(s3Client, dynamoClient).initialise(instanceProperties, tableProperties, configBucket, new Configuration());

        // Then
        assertThat(dynamoClient.scan(
                new ScanRequest().withTableName("sleeper-" + instanceId + "-table-mytable-partitions")).getCount())
                .isEqualTo(Integer.valueOf(7));
        validateSplits(tableProperties, "key", Integer.MIN_VALUE, dynamoClient, 100, 1000, 10000);
    }

    @Test
    public void shouldInitialiseWithBase64EncodedByteArraySplitPoints() throws IOException, StateStoreException {
        // Given
        AmazonS3 s3Client = getS3Client();
        AmazonDynamoDB dynamoClient = getDynamoClient();

        String instanceId = UUID.randomUUID().toString();
        String configBucket = ("sleeper-" + instanceId + "-config").toLowerCase();
        String tableName = "MyTable";
        s3Client.createBucket(configBucket);
        String content = String.join("\n",
                new String(Base64.encodeBase64("a".getBytes(StandardCharsets.UTF_16))),
                new String(Base64.encodeBase64("b".getBytes(StandardCharsets.UTF_16))),
                new String(Base64.encodeBase64("c".getBytes(StandardCharsets.UTF_16))));
        s3Client.putObject(configBucket, "splits/" + tableName, content);

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, instanceId);
        instanceProperties.set(CONFIG_BUCKET, configBucket);

        TableCreator tableCreator = new TableCreator(s3Client, dynamoClient, instanceProperties);
        TableProperties tableProperties = new TableProperties(instanceProperties);
        Schema schema = schemaWithKeyValueTypes(new ByteArrayType(), new StringType());
        tableProperties.setSchema(schema);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(SPLIT_POINTS_BASE64_ENCODED, "true");
        tableProperties.set(SPLIT_POINTS_KEY, "splits/" + tableName);

        tableCreator.createTable(tableProperties);

        // When
        new TableInitialiser(s3Client, dynamoClient).initialise(instanceProperties, tableProperties, configBucket, new Configuration());

        // Then
        assertThat(dynamoClient.scan(
                new ScanRequest().withTableName("sleeper-" + instanceId + "-table-mytable-partitions")).getCount())
                .isEqualTo(Integer.valueOf(7));
        validateSplits(tableProperties, "key", new byte[0], dynamoClient,
                "a".getBytes(StandardCharsets.UTF_16),
                "b".getBytes(StandardCharsets.UTF_16),
                "c".getBytes(StandardCharsets.UTF_16));
    }

    private void validateSplits(TableProperties tableProperties, String fieldName, Object minValue, AmazonDynamoDB dynamoClient, Object... splits) throws StateStoreException {
        DynamoDBStateStore stateStore = new DynamoDBStateStore(tableProperties, dynamoClient);
        assertThat(stateStore.getLeafPartitions()).hasSize(4);

        List<Partition> leafPartitions = stateStore.getLeafPartitions();
        ensurePartitionExists(leafPartitions, fieldName, minValue, splits[0]);
        for (int i = 1; i < splits.length; i++) {
            ensurePartitionExists(leafPartitions, fieldName, splits[i - 1], splits[i]);
        }
        ensurePartitionExists(leafPartitions, fieldName, splits[splits.length - 1], null);
    }

    private void ensurePartitionExists(List<Partition> leafPartitions, String fieldName, Object min, Object max) {
        for (Partition leafPartition : leafPartitions) {
            if (Key.create(leafPartition.getRegion().getRange(fieldName).getMin()).equals(Key.create(min))
                    && Key.create(leafPartition.getRegion().getRange(fieldName).getMax()).equals(Key.create(max))) {
                return;
            }
        }

        fail("Expected to see that one of the partitions would have the min key of \"" + min + "\" and" +
                " the max key of \"" + max + "\". No such partitions were found. they were: \n" +
                leafPartitions.stream().map(Partition::toString).collect(Collectors.joining("\n")));
    }
}
