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

package sleeper.clients.admin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.admin.testutils.AdminClientITBase;
import sleeper.configuration.properties.InstanceProperties;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FARGATE_VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.TASK_RUNNER_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.local.LoadLocalProperties.loadInstancePropertiesFromDirectory;
import static sleeper.configuration.properties.local.LoadLocalProperties.loadTablesFromDirectory;
import static sleeper.configuration.properties.table.TableProperties.TABLES_PREFIX;
import static sleeper.configuration.properties.table.TableProperty.ROW_GROUP_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class AdminConfigStoreIT extends AdminClientITBase {

    private final InstanceProperties instanceProperties = createValidInstanceProperties();

    @BeforeEach
    void setUp() throws IOException {
        instanceProperties.saveToS3(s3);
    }

    @DisplayName("Update instance properties")
    @Nested
    class UpdateInstanceProperties {

        @Test
        void shouldUpdateInstancePropertyInS3() {
            // When
            store().updateInstanceProperty(INSTANCE_ID, FARGATE_VERSION, "1.2.3");

            // Then
            assertThat(store().loadInstanceProperties(INSTANCE_ID).get(FARGATE_VERSION))
                    .isEqualTo("1.2.3");
        }

        @Test
        void shouldUpdateInstancePropertyInLocalDirectory() {
            // When
            store().updateInstanceProperty(INSTANCE_ID, FARGATE_VERSION, "1.2.3");

            // Then
            assertThat(loadInstancePropertiesFromDirectory(tempDir).get(FARGATE_VERSION))
                    .isEqualTo("1.2.3");
        }

        @Test
        void shouldIncludeTableInLocalDirectory() throws IOException {
            // Given
            createTableInS3("test-table");

            // When
            store().updateInstanceProperty(INSTANCE_ID, FARGATE_VERSION, "1.2.3");

            // Then
            assertThat(loadTablesFromDirectory(instanceProperties, tempDir))
                    .extracting(table -> table.get(TABLE_NAME))
                    .containsExactly("test-table");
        }

        @Test
        void shouldRemoveDeletedTableFromLocalDirectoryWhenInstancePropertyIsUpdated() throws IOException {
            // Given
            createTableInS3("old-test-table");
            store().updateInstanceProperty(INSTANCE_ID, FARGATE_VERSION, "1.2.3");
            deleteTableInS3("old-test-table");
            createTableInS3("new-test-table");

            // When
            store().updateInstanceProperty(INSTANCE_ID, FARGATE_VERSION, "4.5.6");

            // Then
            assertThat(loadTablesFromDirectory(instanceProperties, tempDir))
                    .extracting(table -> table.get(TABLE_NAME))
                    .containsExactly("new-test-table");
        }
    }

    @DisplayName("Deploy with CDK")
    @Nested
    class DeployWithCdk {
        @Test
        void shouldRunCdkDeployWhenCdkFlaggedInstancePropertyUpdated() throws IOException, InterruptedException {
            // When
            store().updateInstanceProperty(INSTANCE_ID, TASK_RUNNER_LAMBDA_MEMORY_IN_MB, "123");

            // Then
            verify(cdk).deploy();
        }

        @Test
        void shouldNotRunCdkDeployWhenUnflaggedInstancePropertyUpdated() {
            // When
            store().updateInstanceProperty(INSTANCE_ID, FARGATE_VERSION, "1.2.3");

            // Then
            verifyNoInteractions(cdk);
        }
    }

    @DisplayName("Update table properties")
    @Nested
    class UpdateTableProperties {

        @Test
        void shouldUpdateTablePropertyInS3() throws IOException {
            // Given
            createTableInS3("test-table");

            // When
            store().updateTableProperty(INSTANCE_ID, "test-table", ROW_GROUP_SIZE, "123");

            // Then
            assertThat(store().loadTableProperties(INSTANCE_ID, "test-table").getInt(ROW_GROUP_SIZE))
                    .isEqualTo(123);
        }

        @Test
        void shouldUpdateTablePropertyInLocalDirectory() throws IOException {
            // Given
            createTableInS3("test-table");

            // When
            store().updateTableProperty(INSTANCE_ID, "test-table", ROW_GROUP_SIZE, "123");

            // Then
            assertThat(loadTablesFromDirectory(instanceProperties, tempDir))
                    .extracting(table -> table.getInt(ROW_GROUP_SIZE))
                    .containsExactly(123);
        }

        @Test
        void shouldIncludeNotUpdatedTableInLocalDirectory() throws IOException {
            // Given
            createTableInS3("test-table");
            createTableInS3("test-table-2");

            // When
            store().updateTableProperty(INSTANCE_ID, "test-table", ROW_GROUP_SIZE, "123");

            // Then
            assertThat(loadTablesFromDirectory(instanceProperties, tempDir))
                    .extracting(table -> table.get(TABLE_NAME))
                    .containsExactly("test-table", "test-table-2");
        }

        @Test
        void shouldRemoveDeletedTableFromLocalDirectoryWhenTablePropertyIsUpdated() throws IOException {
            // Given
            createTableInS3("old-test-table");
            store().updateTableProperty(INSTANCE_ID, "old-test-table", ROW_GROUP_SIZE, "123");
            deleteTableInS3("old-test-table");
            createTableInS3("new-test-table");

            // When
            store().updateTableProperty(INSTANCE_ID, "new-test-table", ROW_GROUP_SIZE, "456");

            // Then
            assertThat(loadTablesFromDirectory(instanceProperties, tempDir))
                    .extracting(table -> table.get(TABLE_NAME))
                    .containsExactly("new-test-table");
        }
    }

    private void createTableInS3(String tableName) throws IOException {
        createValidTableProperties(instanceProperties, tableName).saveToS3(s3);
    }

    private void deleteTableInS3(String tableName) {
        s3.deleteObject(CONFIG_BUCKET_NAME, TABLES_PREFIX + "/" + tableName);
    }

}
