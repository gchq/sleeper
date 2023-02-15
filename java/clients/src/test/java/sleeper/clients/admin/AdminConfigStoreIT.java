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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.admin.testutils.AdminClientITBase;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FARGATE_VERSION;
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
            createValidTableProperties(instanceProperties, "test-table").saveToS3(s3);

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

    @DisplayName("Update table properties")
    @Nested
    class UpdateTableProperties {

        private final TableProperties tableProperties = createValidTableProperties(instanceProperties);

        @BeforeEach
        void setUp() throws IOException {
            tableProperties.saveToS3(s3);
        }

        @Test
        void shouldUpdateTableProperty() {
            // When
            store().updateTableProperty(INSTANCE_ID, TABLE_NAME_VALUE, ROW_GROUP_SIZE, "123");

            // Then
            assertThat(store().loadTableProperties(INSTANCE_ID, TABLE_NAME_VALUE).getInt(ROW_GROUP_SIZE))
                    .isEqualTo(123);
        }

        @Test
        @Disabled("TODO")
        void shouldUpdateTablePropertyInLocalDirectory() {
            // When
            store().updateTableProperty(INSTANCE_ID, TABLE_NAME_VALUE, ROW_GROUP_SIZE, "123");

            // Then
            assertThat(loadTablesFromDirectory(instanceProperties, tempDir))
                    .extracting(table -> table.getInt(ROW_GROUP_SIZE))
                    .containsExactly(123);
        }
    }

    private void createTableInS3(String tableName) throws IOException {
        createValidTableProperties(instanceProperties, tableName).saveToS3(s3);
    }

    private void deleteTableInS3(String tableName) {
        s3.deleteObject(CONFIG_BUCKET_NAME, TABLES_PREFIX + "/" + tableName);
    }

}
