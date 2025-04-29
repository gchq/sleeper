/*
 * Copyright 2022-2025 Crown Copyright
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

package sleeper.configurationv2.properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.schema.Schema;
import sleeper.localstack.test.LocalStackTestBase;

import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FORCE_RELOAD_PROPERTIES;
import static sleeper.core.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

class S3PropertiesReloaderIT extends LocalStackTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TablePropertiesStore tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3ClientV2, dynamoClient);

    @BeforeEach
    void setUp() {
        s3ClientV2.createBucket(builder -> builder.bucket(instanceProperties.get(CONFIG_BUCKET)));
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
    }

    @Test
    void shouldReloadInstancePropertiesIfForceReloadPropertiesSetToTrue() {
        // Given
        instanceProperties.set(FORCE_RELOAD_PROPERTIES, "true");
        instanceProperties.set(MAXIMUM_CONNECTIONS_TO_S3, "42");
        S3InstanceProperties.saveToS3(s3ClientV2, instanceProperties);
        updatePropertiesInS3(instanceProperties, properties -> properties.set(MAXIMUM_CONNECTIONS_TO_S3, "26"));
        PropertiesReloader reloader = S3PropertiesReloader.ifConfigured(s3ClientV2, instanceProperties);

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
        S3InstanceProperties.saveToS3(s3ClientV2, instanceProperties);
        updatePropertiesInS3(instanceProperties, properties -> properties.set(MAXIMUM_CONNECTIONS_TO_S3, "26"));
        PropertiesReloader reloader = S3PropertiesReloader.ifConfigured(s3ClientV2, instanceProperties);

        // When
        reloader.reloadIfNeeded();

        // Then
        assertThat(instanceProperties.getInt(MAXIMUM_CONNECTIONS_TO_S3)).isEqualTo(42);
    }

    @Test
    void shouldReloadTablePropertiesIfForceReloadPropertiesSetToTrue() {
        // Given
        instanceProperties.set(FORCE_RELOAD_PROPERTIES, "true");
        S3InstanceProperties.saveToS3(s3ClientV2, instanceProperties);
        String tableName = createTestTable(createSchemaWithKey("key"),
                properties -> properties.set(PARTITION_SPLIT_THRESHOLD, "123"))
                .get(TABLE_NAME);
        updatePropertiesInS3(tableName,
                properties -> properties.set(PARTITION_SPLIT_THRESHOLD, "456"));
        TablePropertiesProvider provider = S3TableProperties.createProvider(instanceProperties, s3ClientV2, dynamoClient);
        provider.getByName(tableName);
        PropertiesReloader reloader = S3PropertiesReloader.ifConfigured(s3ClientV2, instanceProperties, provider);

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
        S3InstanceProperties.saveToS3(s3ClientV2, instanceProperties);
        String tableName = createTestTable(createSchemaWithKey("key"),
                properties -> properties.set(PARTITION_SPLIT_THRESHOLD, "123"))
                .get(TABLE_NAME);
        TablePropertiesProvider provider = S3TableProperties.createProvider(instanceProperties, s3ClientV2, dynamoClient);
        provider.getByName(tableName);
        updatePropertiesInS3(tableName,
                properties -> properties.set(PARTITION_SPLIT_THRESHOLD, "456"));
        PropertiesReloader reloader = S3PropertiesReloader.ifConfigured(s3ClientV2, instanceProperties, provider);

        // When
        reloader.reloadIfNeeded();

        // Then
        assertThat(provider.getByName(tableName)
                .getInt(PARTITION_SPLIT_THRESHOLD))
                .isEqualTo(123);
    }

    private void updatePropertiesInS3(
            InstanceProperties propertiesBefore, Consumer<InstanceProperties> extraProperties) {
        InstanceProperties propertiesAfter = S3InstanceProperties.loadFromBucket(s3ClientV2, propertiesBefore.get(CONFIG_BUCKET));
        extraProperties.accept(propertiesAfter);
        S3InstanceProperties.saveToS3(s3ClientV2, propertiesAfter);
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
