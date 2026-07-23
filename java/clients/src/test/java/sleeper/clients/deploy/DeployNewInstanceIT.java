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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.nio.file.Files.createDirectory;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class DeployNewInstanceIT {
    InstanceProperties instanceProperties = generateInstancePropertiesForFile();
    Schema schema = createSchemaWithKey("key");
    InMemoryTableIndex tableIndex = new InMemoryTableIndex();
    TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStore(tableIndex);
    StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(instanceProperties,
            new InMemoryTransactionLogsPerTable());
    Map<Path, String> pathToString = new HashMap<>();
    List<DeployInstanceRequest> deployRequests = new ArrayList<>();
    Path instancePropertiesFile;
    String configDir;
    String instanceId = "someInstance";
    String vpcId = "someVpc";
    String subnets = "someSubnet1,someSubnet2";

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
            deployNewInstanceWithoutTables(instanceId, vpcId, subnets, "--instance-properties",
                    instancePropertiesFile.toString());

            //Then
            SleeperInstanceConfiguration config = SleeperInstanceConfiguration.fromLocalConfiguration(instancePropertiesFile);
            updatePropertyFiles(config);

            //Verify CDK Command
            assertThat(deployRequests.size()).isEqualTo(1);
            DeployInstanceRequest lastDeployRequest = deployRequests.get(0);
            assertThat(lastDeployRequest).usingRecursiveComparison()
                    .isEqualTo(buildExpectedCDKCommandWithPropertyFile(config, false));

            //Verify no table properties stored
            assertThat(tableIndex.streamAllTables()).isEmpty();
        }

        @Test
        void shouldDeployNewInstanceWhenUsingConfigDir() throws Exception {
            //When
            deployNewInstanceWithTables(instanceId, vpcId, subnets, "--config-dir",
                    configDir);

            //Then
            SleeperInstanceConfiguration config = SleeperInstanceConfiguration.fromLocalConfigurationDirectory(tempDir);
            updatePropertyFiles(config);
            config.getTableProperties().get(0).set(TABLE_ID, tableId("file-table"));

            //Verify CDK Command
            assertThat(deployRequests.size()).isEqualTo(1);
            DeployInstanceRequest lastDeployRequest = deployRequests.get(0);
            assertThat(lastDeployRequest).usingRecursiveComparison()
                    .isEqualTo(buildExpectedCDKCommandWithConfigDir(config, false));

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
            deployNewInstanceWithoutTables(instanceId, vpcId, subnets, "--config-dir", configDir,
                    "--ignoreTableFiles");

            //Then
            SleeperInstanceConfiguration config = SleeperInstanceConfiguration.fromLocalConfiguration(instancePropertiesFile);
            updatePropertyFiles(config);

            //Verify CDK Command
            assertThat(deployRequests.size()).isEqualTo(1);
            DeployInstanceRequest lastDeployRequest = deployRequests.get(0);
            assertThat(lastDeployRequest).usingRecursiveComparison()
                    .isEqualTo(buildExpectedCDKCommandWithPropertyFile(config, false));

            //Verify no table properties stored
            assertThat(tableIndex.streamAllTables()).isEmpty();
        }

        @Test
        void shouldDeployNewInstancePaused() throws Exception {
            //When
            deployNewInstanceWithTables(instanceId, vpcId, subnets, "--config-dir", configDir,
                    "--paused");

            //Then
            SleeperInstanceConfiguration config = SleeperInstanceConfiguration.fromLocalConfigurationDirectory(instancePropertiesFile);
            updatePropertyFiles(config);
            config.getTableProperties().get(0).set(TABLE_ID, tableId("file-table"));

            //Verify CDK Command
            assertThat(deployRequests.size()).isEqualTo(1);
            DeployInstanceRequest lastDeployRequest = deployRequests.get(0);
            assertThat(lastDeployRequest).usingRecursiveComparison()
                    .isEqualTo(buildExpectedCDKCommandWithConfigDir(config, true));

            //Verify Table properties store saved
            TableProperties expected = new TableProperties(instanceProperties);
            expected.setSchema(schema);
            expected.set(TABLE_ID, tableId("file-table"));
            expected.set(TABLE_NAME, "file-table");
            assertThat(tablePropertiesStore.streamAllTables()).containsExactly(expected);
        }

        private void updatePropertyFiles(SleeperInstanceConfiguration config) {
            instanceProperties.set(ID, instanceId);
            instanceProperties.set(VPC_ID, vpcId);
            instanceProperties.set(SUBNETS, subnets);
            config.getInstanceProperties().set(ID, instanceId);
            config.getInstanceProperties().set(VPC_ID, vpcId);
            config.getInstanceProperties().set(SUBNETS, subnets);
        }

        private DeployInstanceRequest buildExpectedCDKCommandWithPropertyFile(SleeperInstanceConfiguration config, boolean deployPaused) {
            CdkCommand cdkCommand = deployPaused ? CdkCommand.deployNewPaused() : CdkCommand.deployNew();
            return DeployInstanceRequest.builder()
                    .instanceConfig(config)
                    .cdkCommand(cdkCommand.withPropertiesFile(instancePropertiesFile)
                            .withNetworkConfiguration(instanceId, vpcId, subnets))
                    .cdkApp(SleeperInternalCdkApp.STANDARD)
                    .build();
        }

        private DeployInstanceRequest buildExpectedCDKCommandWithConfigDir(SleeperInstanceConfiguration config, boolean deployPaused) {
            CdkCommand cdkCommand = deployPaused ? CdkCommand.deployNewPaused() : CdkCommand.deployNew();
            return DeployInstanceRequest.builder()
                    .instanceConfig(config)
                    .cdkCommand(cdkCommand.withConfigurationDirectory(tempDir)
                            .withNetworkConfiguration(instanceId, vpcId, subnets))
                    .cdkApp(SleeperInternalCdkApp.STANDARD)
                    .build();
        }
    }

    @Nested
    class ArgumentsValidation {

        @Test
        void shouldRejectWhenNotEnoughPositionalArguments() {
            // When/Then
            assertThatThrownBy(() -> deployNewInstanceWithoutTables())
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Expected 4 positional arguments, found 1");
        }

        @Test
        void shouldRejectWhenNeitherInstancePropertiesOrConfigDirSet() {
            // When/Then
            assertThatThrownBy(() -> deployNewInstanceWithoutTables("my-instance", "my-vpc", "my-subnets"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Either --instance-properties or --config-dir must be provided");
        }

        @Test
        void shouldRejectWhenBothInstancePropertiesAndConfigDirSet() {
            // When/Then
            assertThatThrownBy(() -> deployNewInstanceWithoutTables("my-instance", "my-vpc", "my-subnets",
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

    private void deployNewInstanceWithoutTables(String... args) throws Exception {
        deployNewInstance(true, args);
    }

    private void deployNewInstanceWithTables(String... args) throws Exception {
        deployNewInstance(false, args);
    }

    private void deployNewInstance(boolean isWithTables, String... args) throws Exception {
        var arguments = DeployNewInstance.readArguments(CommandArgumentReader.parse(DeployNewInstance.USAGE,
                Stream.concat(Stream.of("scriptsDir"), Arrays.stream(args)).toArray(String[]::new)));
        var config = DeployNewInstance.loadConfiguration(arguments);

        DeployNewInstance.Builder builder = DeployNewInstance.builder()
                .deployInstance(request -> deployRequests.add(request))
                .storeFactory(new DeployNewInstance.StoreFactory() {
                    public TablePropertiesStore createTableStore(InstanceProperties p) {
                        return tablePropertiesStore;
                    }

                    public StateStoreProvider createStateStore(InstanceProperties p) {
                        return stateStoreProvider;
                    }

                    public void reloadInstanceProperties(InstanceProperties p) {
                    }
                })
                .deployInstanceConfiguration(config)
                .cdkApp(SleeperInternalCdkApp.STANDARD)
                .ignoreTableFiles(arguments.ignoreTableFiles())
                .deployPaused(arguments.deployPaused());

        if (isWithTables) {
            builder.propertiesFile(instancePropertiesFile);
        } else {
            builder.configDir(tempDir);
        }

        builder.build().deploy();
    }

    private String tableId(String tableName) {
        return tableIndex.getTableByName(tableName)
                .orElseThrow(() -> new RuntimeException("Found tables: " + tableIndex.streamAllTables().toList()))
                .getTableUniqueId();
    }

    private static InstanceProperties generateInstancePropertiesForFile() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.unset(ID);
        instanceProperties.unset(VPC_ID);
        instanceProperties.unset(SUBNETS);
        return instanceProperties;
    }
}
