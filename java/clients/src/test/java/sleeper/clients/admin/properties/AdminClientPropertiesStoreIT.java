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

package sleeper.clients.admin.properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.admin.testutils.AdminClientITBase;
import sleeper.clients.deploy.container.DockerImageConfiguration;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.model.OptionalStack;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.commandsToLoginDockerAndPushImages;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.FARGATE_VERSION;
import static sleeper.core.properties.instance.CommonProperty.FORCE_RELOAD_PROPERTIES;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.local.LoadLocalProperties.loadInstancePropertiesFromDirectory;
import static sleeper.core.properties.local.LoadLocalProperties.loadTablesFromDirectory;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.core.properties.table.TableProperty.ROW_GROUP_SIZE;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class AdminClientPropertiesStoreIT extends AdminClientITBase {

    private final InstanceProperties instanceProperties = createValidInstanceProperties();

    @BeforeEach
    void setUp() {
        setInstanceProperties(instanceProperties);
    }

    @DisplayName("Update instance properties")
    @Nested
    class UpdateInstanceProperties {

        @Test
        void shouldUpdateInstancePropertyInS3() {
            // When
            updateInstanceProperty(instanceId, FARGATE_VERSION, "1.2.3");

            // Then
            assertThat(store().loadInstanceProperties(instanceId).get(FARGATE_VERSION))
                    .isEqualTo("1.2.3");
        }

        @Test
        void shouldUpdateInstancePropertyInLocalDirectory() {
            // When
            updateInstanceProperty(instanceId, FARGATE_VERSION, "1.2.3");

            // Then
            assertThat(loadInstancePropertiesFromDirectory(tempDir).get(FARGATE_VERSION))
                    .isEqualTo("1.2.3");
        }

        @Test
        void shouldIncludeTableInLocalDirectory() {
            // Given
            createTableInS3("test-table");

            // When
            updateInstanceProperty(instanceId, FARGATE_VERSION, "1.2.3");

            // Then
            assertThat(loadTablesFromDirectory(instanceProperties, tempDir))
                    .extracting(table -> table.get(TABLE_NAME))
                    .containsExactly("test-table");
        }

        @Test
        void shouldRemoveDeletedTableFromLocalDirectoryWhenInstancePropertyIsUpdated() {
            // Given
            createTableInS3("old-test-table");
            updateInstanceProperty(instanceId, FARGATE_VERSION, "1.2.3");
            deleteTableInS3("old-test-table");
            createTableInS3("new-test-table");

            // When
            updateInstanceProperty(instanceId, FARGATE_VERSION, "4.5.6");

            // Then
            assertThat(loadTablesFromDirectory(instanceProperties, tempDir))
                    .extracting(table -> table.get(TABLE_NAME))
                    .containsExactly("new-test-table");
        }
    }

    @DisplayName("Update table properties")
    @Nested
    class UpdateTableProperties {

        @Test
        void shouldUpdateTablePropertyInS3() {
            // Given
            createTableInS3("test-table");

            // When
            updateTableProperty(instanceId, "test-table", ROW_GROUP_SIZE, "123");

            // Then
            assertThat(store().loadTableProperties(instanceProperties, "test-table").getInt(ROW_GROUP_SIZE))
                    .isEqualTo(123);
        }

        @Test
        void shouldUpdateTablePropertyInLocalDirectory() {
            // Given
            createTableInS3("test-table");

            // When
            updateTableProperty(instanceId, "test-table", ROW_GROUP_SIZE, "123");

            // Then
            assertThat(loadTablesFromDirectory(instanceProperties, tempDir))
                    .extracting(table -> table.getInt(ROW_GROUP_SIZE))
                    .containsExactly(123);
        }

        @Test
        void shouldIncludeNotUpdatedTableInLocalDirectory() {
            // Given
            createTableInS3("test-table");
            createTableInS3("test-table-2");

            // When
            updateTableProperty(instanceId, "test-table", ROW_GROUP_SIZE, "123");

            // Then
            assertThat(loadTablesFromDirectory(instanceProperties, tempDir))
                    .extracting(table -> table.get(TABLE_NAME))
                    .containsExactly("test-table", "test-table-2");
        }

        @Test
        void shouldRemoveDeletedTableFromLocalDirectoryWhenTablePropertyIsUpdated() {
            // Given
            createTableInS3("old-test-table");
            updateTableProperty(instanceId, "old-test-table", ROW_GROUP_SIZE, "123");
            deleteTableInS3("old-test-table");
            createTableInS3("new-test-table");

            // When
            updateTableProperty(instanceId, "new-test-table", ROW_GROUP_SIZE, "456");

            // Then
            assertThat(loadTablesFromDirectory(instanceProperties, tempDir))
                    .extracting(table -> table.get(TABLE_NAME))
                    .containsExactly("new-test-table");
        }
    }

    @DisplayName("Deploy instance property change with CDK")
    @Nested
    class DeployWithCdk {
        @Test
        void shouldRunCdkDeployWithLocalPropertiesFilesWhenCdkFlaggedInstancePropertyUpdated() throws Exception {
            // Given
            createTableInS3("test-table");
            AtomicReference<InstanceProperties> localPropertiesWhenCdkDeployed = new AtomicReference<>();
            List<TableProperties> localTablesWhenCdkDeployed = new ArrayList<>();
            rememberLocalPropertiesWhenCdkDeployed(localPropertiesWhenCdkDeployed, localTablesWhenCdkDeployed);

            // When
            updateInstanceProperty(instanceId, TASK_RUNNER_LAMBDA_MEMORY_IN_MB, "123");

            // Then
            instanceProperties.set(TASK_RUNNER_LAMBDA_MEMORY_IN_MB, "123");
            verifyPropertiesDeployedWithCdk();
            assertThat(localPropertiesWhenCdkDeployed.get().get(TASK_RUNNER_LAMBDA_MEMORY_IN_MB))
                    .isEqualTo("123");
            assertThat(localTablesWhenCdkDeployed)
                    .extracting(table -> table.get(TABLE_NAME))
                    .containsExactly("test-table");
        }

        @Test
        void shouldNotRunCdkDeployWhenUnflaggedInstancePropertyUpdated() {
            // When
            updateInstanceProperty(instanceId, FARGATE_VERSION, "1.2.3");

            // Then
            verifyNoInteractions(cdk);
        }

        @Test
        void shouldLeaveCdkToUpdateS3WhenApplyingChangeWithCdk() throws Exception {
            // Given
            instanceProperties.set(TASK_RUNNER_LAMBDA_MEMORY_IN_MB, "123");
            S3InstanceProperties.saveToS3(s3, instanceProperties);

            // When
            updateInstanceProperty(instanceId, TASK_RUNNER_LAMBDA_MEMORY_IN_MB, "456");

            // Then
            verifyAnyPropertiesDeployedWithCdk();
            assertThat(store().loadInstanceProperties(instanceId)
                    .get(TASK_RUNNER_LAMBDA_MEMORY_IN_MB))
                    .isEqualTo("123");
        }

        @Test
        void shouldFailWhenCdkDeployFails() throws Exception {
            // Given
            IOException thrown = new IOException("CDK failed");
            doThrowWhenPropertiesDeployedWithCdk(thrown);

            // When / Then
            assertThatThrownBy(() -> updateInstanceProperty(
                    instanceId, TASK_RUNNER_LAMBDA_MEMORY_IN_MB, "456"))
                    .isInstanceOf(AdminClientPropertiesStore.CouldNotSaveInstanceProperties.class)
                    .cause().isSameAs(thrown);
        }

        @Test
        void shouldResetLocalPropertiesWhenCdkDeployFails() throws Exception {
            // Given
            instanceProperties.set(TASK_RUNNER_LAMBDA_MEMORY_IN_MB, "123");
            S3InstanceProperties.saveToS3(s3, instanceProperties);
            doThrowWhenPropertiesDeployedWithCdk(new IOException("CDK failed"));

            // When / Then
            try {
                updateInstanceProperty(instanceId, TASK_RUNNER_LAMBDA_MEMORY_IN_MB, "456");
                fail("CDK failure did not cause an exception");
            } catch (Exception e) {
                assertThat(loadInstancePropertiesFromDirectory(tempDir).get(TASK_RUNNER_LAMBDA_MEMORY_IN_MB))
                        .isEqualTo("123");
            }
        }
    }

    @DisplayName("Load invalid properties")
    @Nested
    class LoadInvalidProperties {

        @Test
        void shouldLoadInvalidInstanceProperties() {
            // Given
            updateInstanceProperty(instanceId, FORCE_RELOAD_PROPERTIES, "abc");

            // When / Then
            assertThat(store().loadInstanceProperties(instanceId).get(FORCE_RELOAD_PROPERTIES))
                    .isEqualTo("abc");
        }

        @Test
        void shouldLoadInvalidTableProperties() {
            // Given
            createTableInS3("test-table", table -> table.set(PARTITION_SPLIT_THRESHOLD, "abc"));

            // When / Then
            assertThat(store().loadTableProperties(instanceProperties, "test-table").get(PARTITION_SPLIT_THRESHOLD))
                    .isEqualTo("abc");
        }
    }

    @DisplayName("Create generated directory when missing")
    @Nested
    class CreateGeneratedDirectoryWhenMissing {

        @Test
        void shouldCreateGeneratedDirectoryWhenSavingInstanceProperties() {
            // Given
            Path generatedDir = tempDir.resolve("dir-to-create");

            // When
            updateInstanceProperty(storeWithGeneratedDirectory(generatedDir),
                    instanceId, FARGATE_VERSION, "1.2.3");

            // Then
            assertThat(loadInstancePropertiesFromDirectory(generatedDir).get(FARGATE_VERSION))
                    .isEqualTo("1.2.3");
        }

        @Test
        void shouldCreateGeneratedDirectoryWhenSavingTableProperties() {
            // Given
            Path generatedDir = tempDir.resolve("dir-to-create");
            createTableInS3("test-table");

            // When
            updateTableProperty(storeWithGeneratedDirectory(generatedDir),
                    instanceId, "test-table", ROW_GROUP_SIZE, "123");

            // Then
            assertThat(loadTablesFromDirectory(instanceProperties, generatedDir))
                    .extracting(properties -> properties.getInt(ROW_GROUP_SIZE))
                    .containsExactly(123);
        }
    }

    @Nested
    @DisplayName("Upload docker images")
    class UploadDockerImages {
        @BeforeEach
        void setup() {
            dockerImageConfiguration = DockerImageConfiguration.getDefault();
            instanceProperties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.QueryStack, OptionalStack.CompactionStack));
            ecrClient.addVersionToRepository(instanceId + "/compaction-job-execution", instanceProperties.get(VERSION));
            ecrClient.addVersionToRepository(instanceId + "/query-lambda", instanceProperties.get(VERSION));
            S3InstanceProperties.saveToS3(s3, instanceProperties);
        }

        @Test
        void shouldUploadDockerImagesWhenOneStackEnabled() throws IOException, InterruptedException {
            // When
            updateInstanceProperty(instanceId, OPTIONAL_STACKS, "QueryStack,CompactionStack,IngestStack");

            // Then
            assertThat(dockerCommandsThatRan).isEqualTo(commandsToLoginDockerAndPushImages(instanceProperties, "ingest"));
        }

        @Test
        void shouldNotUploadDockerImagesWhenNoNewStacksAreEnabled() {
            // When
            updateInstanceProperty(instanceId, FARGATE_VERSION, "1.2.3");

            // Then
            assertThat(dockerCommandsThatRan).isEmpty();
        }

        @Test
        void shouldNotUploadDockerImagesWhenStackIsDisabled() throws IOException, InterruptedException {
            // When
            updateInstanceProperty(instanceId, OPTIONAL_STACKS, "QueryStack");

            // Then
            assertThat(dockerCommandsThatRan).isEmpty();
        }

        @Test
        void shouldUploadDockerImagesWhenOneStackIsEnabledAndAnotherStackIsDisabled() throws IOException, InterruptedException {
            // When
            updateInstanceProperty(instanceId, OPTIONAL_STACKS, "QueryStack,IngestStack");

            // Then
            assertThat(dockerCommandsThatRan).isEqualTo(commandsToLoginDockerAndPushImages(instanceProperties, "ingest"));
        }

        @Test
        void shouldNotUploadDockerImagesWhenStackIsEnabledThatRequiresNoImage() throws IOException, InterruptedException {
            // When
            updateInstanceProperty(instanceId, OPTIONAL_STACKS, "QueryStack,CompactionStack,GarbageCollectorStack");

            // Then
            assertThat(dockerCommandsThatRan).isEmpty();
        }
    }

    private void updateInstanceProperty(String instanceId, InstanceProperty property, String value) {
        updateInstanceProperty(store(), instanceId, property, value);
    }

    private static void updateInstanceProperty(AdminClientPropertiesStore store, String instanceId, InstanceProperty property, String value) {
        InstanceProperties properties = store.loadInstanceProperties(instanceId);
        String valueBefore = properties.get(property);
        properties.set(property, value);
        store.saveInstanceProperties(properties, new PropertiesDiff(property, valueBefore, value));
    }

    private void updateTableProperty(String instanceId, String tableName, TableProperty property, String value) {
        updateTableProperty(store(), instanceId, tableName, property, value);
    }

    private void updateTableProperty(AdminClientPropertiesStore store, String instanceId, String tableName, TableProperty property, String value) {
        TableProperties properties = store.loadTableProperties(instanceProperties, tableName);
        properties.set(property, value);
        store.saveTableProperties(instanceId, properties);
    }

    private void rememberLocalPropertiesWhenCdkDeployed(
            AtomicReference<InstanceProperties> instancePropertiesHolder,
            List<TableProperties> tablePropertiesHolder) throws IOException, InterruptedException {
        doAnswer(invocation -> {
            InstanceProperties properties = loadInstancePropertiesFromDirectory(tempDir);
            instancePropertiesHolder.set(properties);
            tablePropertiesHolder.clear();
            loadTablesFromDirectory(properties, tempDir).forEach(tablePropertiesHolder::add);
            return null;
        }).when(cdk).invokeInferringType(any(), eq(CdkCommand.deployPropertiesChange(propertiesFile())));
    }

    private void createTableInS3(String tableName) {
        saveTableProperties(createValidTableProperties(instanceProperties, tableName));
    }

    private void createTableInS3(String tableName, Consumer<TableProperties> config) {
        TableProperties tableProperties = createValidTableProperties(instanceProperties, tableName);
        config.accept(tableProperties);
        saveTableProperties(tableProperties);
    }

    private void deleteTableInS3(String tableName) {
        tablePropertiesStore.deleteByName(tableName);
    }

    private void verifyAnyPropertiesDeployedWithCdk() throws Exception {
        verify(cdk).invokeInferringType(any(), eq(CdkCommand.deployPropertiesChange(propertiesFile())));
    }

    private void verifyPropertiesDeployedWithCdk() throws Exception {
        verify(cdk).invokeInferringType(instanceProperties, CdkCommand.deployPropertiesChange(propertiesFile()));
    }

    private void doThrowWhenPropertiesDeployedWithCdk(Throwable throwable) throws Exception {
        doThrow(throwable).when(cdk).invokeInferringType(any(), eq(CdkCommand.deployPropertiesChange(propertiesFile())));
    }
}
