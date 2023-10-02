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
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.instance.InstanceProperties;
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
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStore;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.configuration.properties.table.TableProperty.SPLIT_POINTS_BASE64_ENCODED;
import static sleeper.configuration.properties.table.TableProperty.SPLIT_POINTS_KEY;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class TableInitialiserIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    private final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);

    private Schema schemaWithKeyValueTypes(PrimitiveType rowKeyType, Type valueType) {
        return Schema.builder()
                .rowKeyFields(new Field("key", rowKeyType))
                .valueFields(new Field("value", valueType))
                .build();
    }

    @BeforeEach
    void setUp() {
        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
    }

    private void saveSplitPoints(String splitPoints) {
        String key = "splits/" + tableProperties.get(TABLE_NAME);
        s3.putObject(instanceProperties.get(CONFIG_BUCKET), key, splitPoints);
        tableProperties.set(SPLIT_POINTS_KEY, key);
    }

    private void createTable() {
        new TableCreator(s3, dynamoDB, instanceProperties).createTable(tableProperties);
    }

    private void initialiseTable() {
        new TableInitialiser(s3, dynamoDB).initialise(
                instanceProperties, tableProperties, instanceProperties.get(CONFIG_BUCKET), new Configuration());
    }

    private StateStore stateStore() {
        return new DynamoDBStateStore(instanceProperties, tableProperties, dynamoDB);
    }

    @Test
    public void shouldInitialiseStateStoreWithNoSplitPoints() throws Exception {
        // Given
        tableProperties.setSchema(schemaWithKeyValueTypes(new StringType(), new StringType()));
        createTable();

        // When
        initialiseTable();

        // Then
        assertThat(stateStore().getAllPartitions()).hasSize(1);
    }

    @Test
    public void shouldInitialiseTableWithStringSplitPoints() throws StateStoreException {
        // Given
        saveSplitPoints("a\nb\nc");
        tableProperties.setSchema(schemaWithKeyValueTypes(new StringType(), new StringType()));
        createTable();

        // When
        initialiseTable();

        // Then
        assertThat(stateStore().getAllPartitions()).hasSize(7);
        validateSplits("key", "", "a", "b", "c");
    }

    @Test
    public void shouldInitialiseWithBase64EncodedStringSplitPoints() throws StateStoreException {
        // Given
        String content = String.join("\n", new String(Base64.encodeBase64("a".getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8),
                new String(Base64.encodeBase64("b".getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8),
                new String(Base64.encodeBase64("c".getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8));
        saveSplitPoints(content);
        tableProperties.set(SPLIT_POINTS_BASE64_ENCODED, "true");
        tableProperties.setSchema(schemaWithKeyValueTypes(new StringType(), new StringType()));
        createTable();

        // When
        initialiseTable();

        // Then
        assertThat(stateStore().getAllPartitions()).hasSize(7);
        validateSplits("key", "", "a", "b", "c");
    }

    @Test
    public void shouldInitialiseTableWithLongSplitPoints() throws StateStoreException {
        // Given
        saveSplitPoints("1000\n1000000\n1000000000");
        tableProperties.setSchema(schemaWithKeyValueTypes(new LongType(), new StringType()));
        createTable();

        // When
        initialiseTable();

        // Then
        assertThat(stateStore().getAllPartitions()).hasSize(7);
        validateSplits("key", Long.MIN_VALUE, 1000L, 1_000_000L, 1_000_000_000L);
    }

    @Test
    public void shouldInitialiseTableWithIntegerSplitPoints() throws StateStoreException {
        // Given
        saveSplitPoints("100\n1000\n10000");
        tableProperties.setSchema(schemaWithKeyValueTypes(new IntType(), new StringType()));
        createTable();

        // When
        initialiseTable();

        // Then
        assertThat(stateStore().getAllPartitions()).hasSize(7);
        validateSplits("key", Integer.MIN_VALUE, 100, 1000, 10000);
    }

    @Test
    public void shouldInitialiseWithBase64EncodedByteArraySplitPoints() throws StateStoreException {
        // Given
        String content = String.join("\n",
                new String(Base64.encodeBase64("a".getBytes(StandardCharsets.UTF_16)), StandardCharsets.UTF_8),
                new String(Base64.encodeBase64("b".getBytes(StandardCharsets.UTF_16)), StandardCharsets.UTF_8),
                new String(Base64.encodeBase64("c".getBytes(StandardCharsets.UTF_16)), StandardCharsets.UTF_8));
        saveSplitPoints(content);
        tableProperties.setSchema(schemaWithKeyValueTypes(new ByteArrayType(), new StringType()));
        tableProperties.set(SPLIT_POINTS_BASE64_ENCODED, "true");
        createTable();

        // When
        initialiseTable();

        // Then
        assertThat(stateStore().getAllPartitions()).hasSize(7);
        validateSplits("key", new byte[0],
                "a".getBytes(StandardCharsets.UTF_16),
                "b".getBytes(StandardCharsets.UTF_16),
                "c".getBytes(StandardCharsets.UTF_16));
    }

    private void validateSplits(String fieldName, Object minValue, Object... splits) throws StateStoreException {
        List<Partition> leafPartitions = stateStore().getLeafPartitions();
        assertThat(leafPartitions).hasSize(4);

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
