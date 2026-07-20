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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.clients.util.cdk.CdkCommand;
import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static java.nio.file.Files.createDirectory;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstancePropertiesWithId;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class DeployNewInstanceIT {
    InstanceProperties instanceProperties = createTestInstancePropertiesWithId("my-instance");
    Schema schema = createSchemaWithKey("key");
    InMemoryTableIndex tableIndex = new InMemoryTableIndex();
    TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStore(tableIndex);
    StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(instanceProperties,
            new InMemoryTransactionLogsPerTable());
    Map<Path, String> pathToString = new HashMap<>();
    DeployInstanceRequest lastDeployRequest;
    Path instancePropertiesFile;
    String configDir;

    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        createTempDirectory(tempDir, null);
        instancePropertiesFile = tempDir.resolve("instance.properties");
        Files.writeString(instancePropertiesFile, instanceProperties.saveAsString());
        Path tables = tempDir.resolve("tables");
        Path table1 = tables.resolve("table1");
        createDirectory(tables);
        createDirectory(table1);
        Files.writeString(table1.resolve("table.properties"), "sleeper.table.name=file-table\n");
        Files.writeString(table1.resolve("schema.json"), new SchemaSerDe().toJson(schema));
        configDir = tempDir.toString();
    }

    @Nested
    class DeployNew {

        @Test
        void shouldDeployNewInstanceWhenUsingInstanceProperties() throws Exception {
            //When
            deployNewInstance("scriptsDir", "someInstance", "someVpc", "someSubnets", "--instance-properties",
                    instancePropertiesFile.toString());

            //Then
            //Verify Instance Properties file updates
            instanceProperties.set(ID, "someInstance");
            instanceProperties.set(VPC_ID, "someVpc");
            instanceProperties.set(SUBNETS, "someSubnets");
            assertThat(instanceProperties)
                    .isEqualTo(SleeperInstanceConfiguration.fromLocalConfiguration(
                            instancePropertiesFile).getInstanceProperties());

            //Verify CDK Command
            assertThat(lastDeployRequest.getCdkCommand()).isEqualTo(CdkCommand.deployNew());
            assertThat(lastDeployRequest.getCdkApp()).isEqualTo(SleeperInternalCdkApp.STANDARD);

            //Verify no table properties stored
            assertThat(tableIndex.streamAllTables()).isEmpty();
        }

        @Test
        void shouldDeployNewInstanceWhenUsingConfigDir() throws Exception {
            //When
            deployNewInstance("scriptsDir", "someInstance", "someVpc", "someSubnets", "--config-dir",
                    configDir);

            //Then
            //Verify Instance Properties file updates
            instanceProperties.set(ID, "someInstance");
            instanceProperties.set(VPC_ID, "someVpc");
            instanceProperties.set(SUBNETS, "someSubnets");
            assertThat(instanceProperties)
                    .isEqualTo(SleeperInstanceConfiguration.fromLocalConfiguration(
                            instancePropertiesFile).getInstanceProperties());

            //Verify CDK Command
            assertThat(lastDeployRequest.getCdkCommand()).isEqualTo(CdkCommand.deployNew());
            assertThat(lastDeployRequest.getCdkApp()).isEqualTo(SleeperInternalCdkApp.STANDARD);

            //Verify Table properties store saved
            TableProperties expected = new TableProperties(instanceProperties);
            expected.setSchema(schema);
            expected.set(TABLE_ID, tableId("file-table"));
            expected.set(TABLE_NAME, "file-table");
            assertThat(tablePropertiesStore.streamAllTables()).containsExactly(expected);
        }

        @Test
        void shouldDeployNewInstanceWhenUsingInstancePropertiesIgnoringTableFiles() throws Exception {
            //When
            deployNewInstance("scriptsDir", "someInstance", "someVpc", "someSubnets", "--config-dir", configDir,
                    "--ignoreTableFiles");

            //Then
            //Verify Instance Properties file updates
            instanceProperties.set(ID, "someInstance");
            instanceProperties.set(VPC_ID, "someVpc");
            instanceProperties.set(SUBNETS, "someSubnets");
            assertThat(instanceProperties)
                    .isEqualTo(SleeperInstanceConfiguration.fromLocalConfiguration(
                            instancePropertiesFile).getInstanceProperties());

            //Verify CDK Command
            assertThat(lastDeployRequest.getCdkCommand()).isEqualTo(CdkCommand.deployNew());
            assertThat(lastDeployRequest.getCdkApp()).isEqualTo(SleeperInternalCdkApp.STANDARD);

            //Verify no table properties stored
            assertThat(tableIndex.streamAllTables()).isEmpty();
        }

        @Test
        void shouldDeployNewInstancePaused() throws Exception {
            //When
            deployNewInstance("scriptsDir", "someInstance", "someVpc", "someSubnets", "--config-dir", configDir,
                    "--paused");

            //Then
            //Verify Instance Properties file updates
            instanceProperties.set(ID, "someInstance");
            instanceProperties.set(VPC_ID, "someVpc");
            instanceProperties.set(SUBNETS, "someSubnets");
            assertThat(instanceProperties)
                    .isEqualTo(SleeperInstanceConfiguration.fromLocalConfiguration(
                            instancePropertiesFile).getInstanceProperties());

            //Verify CDK Command
            assertThat(lastDeployRequest.getCdkCommand()).isEqualTo(CdkCommand.deployNewPaused());
            assertThat(lastDeployRequest.getCdkApp()).isEqualTo(SleeperInternalCdkApp.STANDARD);

            //Verify Table properties store saved
            TableProperties expected = new TableProperties(instanceProperties);
            expected.setSchema(schema);
            expected.set(TABLE_ID, tableId("file-table"));
            expected.set(TABLE_NAME, "file-table");
            assertThat(tablePropertiesStore.streamAllTables()).containsExactly(expected);
        }
    }

    @Nested
    class ArgumentsValidation {

        @Test
        void shouldRejectWhenNotEnoughPositionalArguments() {
            // When/Then
            assertThatThrownBy(() -> deployNewInstance())
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Expected 4 positional arguments, found 0");
        }

        @Test
        void shouldRejectWhenNeitherInstancePropertiesOrConfigDirSet() {
            // When/Then
            assertThatThrownBy(() -> deployNewInstance("scriptsDir", "my-instance", "my-vpc", "my-subnets"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Either --instance-properties or --config-dir must be provided");
        }

        @Test
        void shouldRejectWhenBothInstancePropertiesAndConfigDirSet() {
            // When/Then
            assertThatThrownBy(() -> deployNewInstance("scriptsDir", "my-instance", "my-vpc", "my-subnets",
                    "--instance-properties", "someFile", "--config-dir", "someDir"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Cannot use both --instance-properties and --config-dir");
        }

        @Test
        void shouldSetIgnoreTableFilesTrueWhenInstancePropertiesUsed() {
            var arguments = DeployNewInstance.readArguments(CommandArgumentReader.parse(DeployNewInstance.USAGE,
                    "scriptsDir", "my-instance", "my-vpc", "my-subnets", "--instance-properties", "someFile"));
            assertThat(arguments.ignoreTableFiles()).isTrue();
        }

        @Test
        void shouldResolvePropertiesFileWhenConfigDirUsed() {
            var arguments = DeployNewInstance.readArguments(CommandArgumentReader.parse(DeployNewInstance.USAGE,
                    "scriptsDir", "my-instance", "my-vpc", "my-subnets", "--config-dir", "someDir"));
            assertThat(arguments.resolvePropertiesFile()).isEqualTo(Path.of("someDir/instance.properties"));
        }

    }

    private void deployNewInstance(String... args) throws Exception {
        var arguments = DeployNewInstance.readArguments(CommandArgumentReader.parse(DeployNewInstance.USAGE, args));
        var config = DeployNewInstance.loadAndUpdateConfiguration(arguments);
        new DeployNewInstance(
                request -> lastDeployRequest = request,
                new DeployNewInstance.StoreFactory() {
                    public TablePropertiesStore createTableStore(InstanceProperties p) {
                        return tablePropertiesStore;
                    }

                    public StateStoreProvider createStateStore(InstanceProperties p) {
                        return stateStoreProvider;
                    }

                    public void reloadInstanceProperties(InstanceProperties p) {
                    }
                },
                config, SleeperInternalCdkApp.STANDARD, instancePropertiesFile, tempDir,
                arguments.ignoreTableFiles(), arguments.deployPaused()).deploy();
    }

    private String tableId(String tableName) {
        return tableIndex.getTableByName(tableName)
                .orElseThrow(() -> new RuntimeException("Found tables: " + tableIndex.streamAllTables().toList()))
                .getTableUniqueId();
    }
}
