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
package sleeper.clients.deploy;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.clients.util.cdk.CdkCommand;
import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.LoadLocalProperties;
import sleeper.core.properties.model.SleeperInternalCdkApp;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.util.cli.CommandArgumentReader;
import sleeper.core.util.cli.CommandArgumentsException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.RETAIN_LOGS_AFTER_DESTROY;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class DeployNewInstanceIT {
    InstanceProperties instanceProperties = new InstanceProperties();
    Schema schema = createSchemaWithKey("key");
    InMemoryTableIndex tableIndex = new InMemoryTableIndex();
    TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStore(tableIndex);
    StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(instanceProperties,
            new InMemoryTransactionLogsPerTable());
    Map<Path, String> pathToString = new HashMap<>();
    List<DeployInstanceRequest> deployRequests = new ArrayList<>();

    @TempDir
    private Path configDir;

    @Nested
    class DeployNew {

        @Test
        void shouldDeployNewInstanceWhenUsingInstanceProperties() throws Exception {
            // Given
            instanceProperties.set(RETAIN_LOGS_AFTER_DESTROY, "false");
            Path propertiesFile = writeInstancePropertiesFile();

            // When
            deployNewInstance(
                    "my-instance", "test-vpc", "test-subnet",
                    "--properties-file", propertiesFile.toString());

            // Then the combined configuration is derived
            // And the CDK is invoked pointing to the file and the extra configuration
            InstanceProperties expected = new InstanceProperties();
            expected.set(ID, "my-instance");
            expected.set(VPC_ID, "test-vpc");
            expected.set(SUBNETS, "test-subnet");
            expected.set(RETAIN_LOGS_AFTER_DESTROY, "false");
            assertThat(deployRequests).containsExactly(DeployInstanceRequest.builder()
                    .instanceConfig(SleeperInstanceConfiguration.withNoTables(expected))
                    .cdkCommand(CdkCommand.deployNew().withPropertiesFile(propertiesFile).toBuilder()
                            .instanceId("my-instance")
                            .vpcId("test-vpc")
                            .subnets("test-subnet")
                            .build())
                    .cdkApp(SleeperInternalCdkApp.STANDARD)
                    .build());
            assertThat(tableIndex.streamAllTables()).isEmpty();
            assertThat(LoadLocalProperties.loadInstancePropertiesNoValidation(propertiesFile)).isEqualTo(instanceProperties);
        }

        @Test
        void shouldDeployNewInstanceWhenUsingConfigDir() throws Exception {
            // Given
            instanceProperties.set(RETAIN_LOGS_AFTER_DESTROY, "false");
            writeInstancePropertiesFile();
            TableProperties tableProperties = new TableProperties(instanceProperties);
            tableProperties.set(TABLE_NAME, "test-table");
            tableProperties.setSchema(createSchemaWithKey("key"));
            writeTablePropertiesDir(tableProperties);

            // When
            deployNewInstance(
                    "my-instance", "test-vpc", "test-subnet",
                    "--config-dir", configDir.toString());

            // Then CDK is invoked before AddTableClient runs — tables have no ID in the CDK request
            InstanceProperties expected = new InstanceProperties();
            expected.set(ID, "my-instance");
            expected.set(VPC_ID, "test-vpc");
            expected.set(SUBNETS, "test-subnet");
            expected.set(RETAIN_LOGS_AFTER_DESTROY, "false");
            TableProperties expectedTableForCdk = new TableProperties(expected);
            expectedTableForCdk.set(TABLE_NAME, "test-table");
            expectedTableForCdk.setSchema(createSchemaWithKey("key"));
            expectedTableForCdk.set(TABLE_ID, tableId("test-table"));
            assertThat(deployRequests).containsExactly(DeployInstanceRequest.builder()
                    .instanceConfig(SleeperInstanceConfiguration.builder().instanceProperties(expected).tableProperties(expectedTableForCdk).build())
                    .cdkCommand(CdkCommand.deployNew().withConfigurationDirectory(configDir).toBuilder()
                            .instanceId("my-instance")
                            .vpcId("test-vpc")
                            .subnets("test-subnet")
                            .build())
                    .cdkApp(SleeperInternalCdkApp.STANDARD)
                    .build());
            // AddTableClient runs after CDK and assigns TABLE_ID using the reloaded deployed properties
            InstanceProperties expectedDeployedProperties = new InstanceProperties();
            expectedDeployedProperties.set(RETAIN_LOGS_AFTER_DESTROY, "false");
            expectedDeployedProperties.set(ID, "my-instance");
            expectedDeployedProperties.set(VPC_ID, "test-vpc");
            expectedDeployedProperties.set(SUBNETS, "test-subnet");
            TableProperties expectedCreatedTable = new TableProperties(expectedDeployedProperties);
            expectedCreatedTable.set(TABLE_ID, tableId("test-table"));
            expectedCreatedTable.set(TABLE_NAME, "test-table");
            expectedCreatedTable.setSchema(createSchemaWithKey("key"));
            assertThat(tablePropertiesStore.streamAllTables()).containsExactly(expectedCreatedTable);
        }

        @Test
        void shouldDeployNewInstancePaused() throws Exception {
            // Given
            Path propertiesFile = writeInstancePropertiesFile();

            // When
            deployNewInstance(
                    "my-instance", "test-vpc", "test-subnet",
                    "--properties-file", propertiesFile.toString(),
                    "--paused");

            // Then the combined configuration is derived
            // And the CDK is invoked pointing to the file and the extra configuration
            InstanceProperties expected = new InstanceProperties();
            expected.set(ID, "my-instance");
            expected.set(VPC_ID, "test-vpc");
            expected.set(SUBNETS, "test-subnet");
            assertThat(deployRequests).containsExactly(DeployInstanceRequest.builder()
                    .instanceConfig(SleeperInstanceConfiguration.withNoTables(expected))
                    .cdkCommand(CdkCommand.deployNewPaused().withPropertiesFile(propertiesFile).toBuilder()
                            .instanceId("my-instance")
                            .vpcId("test-vpc")
                            .subnets("test-subnet")
                            .build())
                    .cdkApp(SleeperInternalCdkApp.STANDARD)
                    .build());
            assertThat(tableIndex.streamAllTables()).isEmpty();
            assertThat(LoadLocalProperties.loadInstancePropertiesNoValidation(propertiesFile)).isEqualTo(new InstanceProperties());
        }
    }

    @Nested
    class ArgumentsValidation {

        @Test
        void shouldRejectWhenNotEnoughPositionalArguments() {
            // When/Then
            assertThatThrownBy(() -> deployNewInstance())
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessageContaining("Expected 3 positional arguments, found 0");
        }

        @Test
        void shouldRejectWhenNeitherInstancePropertiesOrConfigDirSet() {
            // When/Then
            assertThatThrownBy(() -> deployNewInstance("my-instance", "my-vpc", "my-subnets"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Either --properties-file or --config-dir must be provided");
        }

        @Test
        void shouldRejectWhenBothInstancePropertiesAndConfigDirSet() {
            // When/Then
            assertThatThrownBy(() -> deployNewInstance("my-instance", "my-vpc", "my-subnets",
                    "--properties-file", "someFile", "--config-dir", "someDir"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Cannot use both --properties-file and --config-dir");
        }
    }

    private void deployNewInstance(String... rawArgs) throws Exception {
        var args = DeployNewInstance.readArguments(CommandArgumentReader.parse(DeployNewInstance.USAGE,
                Stream.concat(Stream.of("scriptsDir"), Arrays.stream(rawArgs)).toArray(String[]::new)));
        var config = DeployNewInstance.loadConfiguration(args);
        InstanceProperties deployedProperties = InstanceProperties.copyOf(instanceProperties);

        DeployNewInstance.builder()
                .deployInstance(request -> deployRequests.add(request))
                .storeFactory(new DeployNewInstance.StoreFactory() {
                    public TablePropertiesStore createTableStore(InstanceProperties p) {
                        return tablePropertiesStore;
                    }

                    public StateStoreProvider createStateStore(InstanceProperties p) {
                        return stateStoreProvider;
                    }
                })
                .instancePropertiesLoader(id -> {
                    deployedProperties.set(ID, config.getInstanceProperties().get(ID));
                    deployedProperties.set(VPC_ID, config.getInstanceProperties().get(VPC_ID));
                    deployedProperties.set(SUBNETS, config.getInstanceProperties().get(SUBNETS));
                    return deployedProperties;
                })
                .expectedInstanceConfiguration(config)
                .cdkApp(SleeperInternalCdkApp.STANDARD)
                .propertiesFile(args.propertiesFile())
                .configDir(args.configDir())
                .deployPaused(args.deployPaused())
                .build().deploy();
    }

    private String tableId(String tableName) {
        return tableIndex.getTableByName(tableName)
                .orElseThrow(() -> new RuntimeException("Found tables: " + tableIndex.streamAllTables().toList()))
                .getTableUniqueId();
    }

    private Path writeInstancePropertiesFile() {
        Path file = configDir.resolve("instance.properties");
        try {
            Files.writeString(file, instanceProperties.toString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return file;
    }

    private void writeTablePropertiesDir(TableProperties tableProperties) {
        Path tablesDir = configDir.resolve("tables");
        Path tableDir = tablesDir.resolve(tableProperties.get(TABLE_NAME));
        try {
            Files.createDirectory(tablesDir);
            Files.createDirectory(tableDir);
            Files.writeString(tableDir.resolve("table.properties"), tableProperties.toString());
            Files.writeString(tableDir.resolve("schema.json"), new SchemaSerDe().toJson(tableProperties.getSchema()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
