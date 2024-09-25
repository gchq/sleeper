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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.configuration.s3properties.PropertiesReloader;
import sleeper.configuration.s3properties.S3InstanceProperties;
import sleeper.configuration.s3properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.schema.Schema;

import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FORCE_RELOAD_PROPERTIES;
import static sleeper.configuration.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

@Testcontainers
public class PropertiesReloaderIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    private final AmazonS3 s3Client = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoClient = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TablePropertiesStore tablePropertiesStore = S3TableProperties.getStore(instanceProperties, s3Client, dynamoClient);

    @BeforeEach
    void setUp() {
        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
    }

    @Test
    void shouldReloadInstancePropertiesIfForceReloadPropertiesSetToTrue() {
        // Given
        instanceProperties.set(FORCE_RELOAD_PROPERTIES, "true");
        instanceProperties.set(MAXIMUM_CONNECTIONS_TO_S3, "42");
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        updatePropertiesInS3(instanceProperties, properties -> properties.set(MAXIMUM_CONNECTIONS_TO_S3, "26"));
        PropertiesReloader reloader = PropertiesReloader.ifConfigured(s3Client, instanceProperties);

        // When
        reloader.reloadIfNeeded();

        // Then
        assertThat(instanceProperties.getInt(MAXIMUM_CONNECTIONS_TO_S3)).isEqualTo(26);
    }

    @Test
    void shouldNotReloadInstancePropertiesIfForceReloadPropertiesSetToFalse() {
        // Given
        instanceProperties.set(FORCE_RELOAD_PROPERTIES, "false");
        instanceProperties.set(MAXIMUM_CONNECTIONS_TO_S3, "42");
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        updatePropertiesInS3(instanceProperties, properties -> properties.set(MAXIMUM_CONNECTIONS_TO_S3, "26"));
        PropertiesReloader reloader = PropertiesReloader.ifConfigured(s3Client, instanceProperties);

        // When
        reloader.reloadIfNeeded();

        // Then
        assertThat(instanceProperties.getInt(MAXIMUM_CONNECTIONS_TO_S3)).isEqualTo(42);
    }

    @Test
    void shouldReloadTablePropertiesIfForceReloadPropertiesSetToTrue() {
        // Given
        instanceProperties.set(FORCE_RELOAD_PROPERTIES, "true");
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        String tableName = createTestTable(schemaWithKey("key"),
                properties -> properties.set(PARTITION_SPLIT_THRESHOLD, "123"))
                .get(TABLE_NAME);
        updatePropertiesInS3(tableName,
                properties -> properties.set(PARTITION_SPLIT_THRESHOLD, "456"));
        TablePropertiesProvider provider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
        provider.getByName(tableName);
        PropertiesReloader reloader = PropertiesReloader.ifConfigured(s3Client, instanceProperties, provider);

        // When
        reloader.reloadIfNeeded();

        // Then
        assertThat(provider.getByName(tableName)
                .getInt(PARTITION_SPLIT_THRESHOLD))
                .isEqualTo(456);
    }

    @Test
    void shouldNotReloadTablePropertiesIfForceReloadPropertiesSetToFalse() {
        // Given
        instanceProperties.set(FORCE_RELOAD_PROPERTIES, "false");
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        String tableName = createTestTable(schemaWithKey("key"),
                properties -> properties.set(PARTITION_SPLIT_THRESHOLD, "123"))
                .get(TABLE_NAME);
        TablePropertiesProvider provider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
        provider.getByName(tableName);
        updatePropertiesInS3(tableName,
                properties -> properties.set(PARTITION_SPLIT_THRESHOLD, "456"));
        PropertiesReloader reloader = PropertiesReloader.ifConfigured(s3Client, instanceProperties, provider);

        // When
        reloader.reloadIfNeeded();

        // Then
        assertThat(provider.getByName(tableName)
                .getInt(PARTITION_SPLIT_THRESHOLD))
                .isEqualTo(123);
    }

    private void updatePropertiesInS3(
            InstanceProperties propertiesBefore, Consumer<InstanceProperties> extraProperties) {
        InstanceProperties propertiesAfter = S3InstanceProperties.loadFromBucket(s3Client, propertiesBefore.get(CONFIG_BUCKET));
        extraProperties.accept(propertiesAfter);
        S3InstanceProperties.saveToS3(s3Client, propertiesAfter);
    }

    private void updatePropertiesInS3(String tableName, Consumer<TableProperties> extraProperties) {
        TableProperties propertiesAfter = tablePropertiesStore.loadByName(tableName);
        extraProperties.accept(propertiesAfter);
        tablePropertiesStore.save(propertiesAfter);
    }

    private TableProperties createTestTable(Schema schema, Consumer<TableProperties> tableConfig) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableConfig.accept(tableProperties);
        tablePropertiesStore.save(tableProperties);
        return tableProperties;
    }
}
