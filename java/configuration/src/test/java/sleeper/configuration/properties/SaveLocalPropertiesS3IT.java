/*
 * Copyright 2022-2026 Crown Copyright
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.localstack.test.LocalStackTestBase;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACCOUNT;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.local.LoadLocalProperties.loadInstanceProperties;
import static sleeper.core.properties.local.LoadLocalProperties.loadTablesFromInstancePropertiesFile;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

class SaveLocalPropertiesS3IT extends LocalStackTestBase {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp() {
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
    }

    private InstanceProperties savePropertiesToTempDirFromS3() throws IOException {
        return S3InstanceProperties.saveToLocalWithTableProperties(s3Client, dynamoClient,
                instanceProperties.get(ACCOUNT), instanceProperties.get(ID), tempDir);
    }

    @Test
    void shouldSaveInstancePropertiesToLocalFile() throws IOException {
        // When
        savePropertiesToTempDirFromS3();

        // Then
        assertThat(loadInstanceProperties(tempDir.resolve("instance.properties")))
                .isEqualTo(instanceProperties);
    }

    @Test
    void shouldSaveTablePropertiesToLocalFiles() throws IOException {
        // Given
        TableProperties table1 = createTestTable(createSchemaWithKey("key1"));
        TableProperties table2 = createTestTable(createSchemaWithKey("key2"));

        // When
        savePropertiesToTempDirFromS3();

        // Then
        assertThat(loadTablesFromInstancePropertiesFile(instanceProperties, tempDir.resolve("instance.properties")))
                .containsExactlyInAnyOrder(table1, table2);
    }

    @Test
    void shouldLoadNoTablePropertiesFromS3WhenNoneAreSaved() throws IOException {
        // When
        savePropertiesToTempDirFromS3();

        // Then
        assertThat(loadTablesFromInstancePropertiesFile(instanceProperties, tempDir.resolve("instance.properties"))).isEmpty();
    }

    @Test
    void shouldReturnInstancePropertiesFromS3() throws IOException {
        // When
        InstanceProperties saved = savePropertiesToTempDirFromS3();

        // Then
        assertThat(saved).isEqualTo(instanceProperties);
    }

    private TableProperties createTestTable(Schema schema) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient).save(tableProperties);
        return tableProperties;
    }
}
