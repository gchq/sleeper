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
    List<DeployInstanceRequest> deployRequests = new ArrayList<>();
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
            deployNewInstanceByPropertiesFile("someInstance", "someVpc", "someSubnets", "--instance-properties",
                    instancePropertiesFile.toString());

            //Then
            //Verify Instance Properties file updates
            instanceProperties.set(ID, "someInstance");
            instanceProperties.set(VPC_ID, "someVpc");
            instanceProperties.set(SUBNETS, "someSubnets");
            SleeperInstanceConfiguration config = SleeperInstanceConfiguration.fromLocalConfiguration(instancePropertiesFile);
            assertThat(config.getInstanceProperties()).isEqualTo(instanceProperties);

            //Verify CDK Command
            assertThat(deployRequests.size()).isEqualTo(1);
            DeployInstanceRequest lastDeployRequest = deployRequests.get(0);
            assertThat(lastDeployRequest).usingRecursiveComparison()
                    .isEqualTo(DeployInstanceRequest.builder()
                            .instanceConfig(config)
                            .cdkCommand(CdkCommand.deployNew())
                            .cdkApp(SleeperInternalCdkApp.STANDARD)
                            .propertiesFile(instancePropertiesFile)
                            .configDir(null)
                            .build());

            //Verify no table properties stored
            assertThat(tableIndex.streamAllTables()).isEmpty();
        }

        @Test
        void shouldDeployNewInstanceWhenUsingConfigDir() throws Exception {
            //When
            deployNewInstanceByConfigDir("someInstance", "someVpc", "someSubnets", "--config-dir",
                    configDir);

            //Then
            //Verify Instance Properties file updates
            instanceProperties.set(ID, "someInstance");
            instanceProperties.set(VPC_ID, "someVpc");
            instanceProperties.set(SUBNETS, "someSubnets");
            SleeperInstanceConfiguration config = SleeperInstanceConfiguration.fromLocalConfigurationDirectory(tempDir);
            config.getTableProperties().get(0).set(TABLE_ID, tableId("file-table"));
            assertThat(config.getInstanceProperties()).isEqualTo(instanceProperties);

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
            deployNewInstanceByConfigDir("someInstance", "someVpc", "someSubnets", "--config-dir", configDir,
                    "--ignoreTableFiles");

            //Then
            //Verify Instance Properties file updates
            instanceProperties.set(ID, "someInstance");
            instanceProperties.set(VPC_ID, "someVpc");
            instanceProperties.set(SUBNETS, "someSubnets");
            SleeperInstanceConfiguration config = SleeperInstanceConfiguration.fromLocalConfiguration(instancePropertiesFile);
            assertThat(config.getInstanceProperties()).isEqualTo(instanceProperties);

            //Verify CDK Command
            assertThat(deployRequests.size()).isEqualTo(1);
            DeployInstanceRequest lastDeployRequest = deployRequests.get(0);
            assertThat(lastDeployRequest).usingRecursiveComparison()
                    .isEqualTo(buildExpectedCDKCommandWithConfigDir(config, false));

            //Verify no table properties stored
            assertThat(tableIndex.streamAllTables()).isEmpty();
        }

        @Test
        void shouldDeployNewInstancePaused() throws Exception {
            //When
            deployNewInstanceByConfigDir("someInstance", "someVpc", "someSubnets", "--config-dir", configDir,
                    "--paused");

            //Then
            //Verify Instance Properties file updates
            instanceProperties.set(ID, "someInstance");
            instanceProperties.set(VPC_ID, "someVpc");
            instanceProperties.set(SUBNETS, "someSubnets");
            SleeperInstanceConfiguration config = SleeperInstanceConfiguration.fromLocalConfigurationDirectory(instancePropertiesFile);
            config.getTableProperties().get(0).set(TABLE_ID, tableId("file-table"));
            assertThat(config.getInstanceProperties()).isEqualTo(instanceProperties);

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

        private DeployInstanceRequest buildExpectedCDKCommandWithConfigDir(SleeperInstanceConfiguration config, boolean deployPaused) {
            return DeployInstanceRequest.builder()
                    .instanceConfig(config)
                    .cdkCommand(deployPaused ? CdkCommand.deployNewPaused() : CdkCommand.deployNew())
                    .cdkApp(SleeperInternalCdkApp.STANDARD)
                    .propertiesFile(null)
                    .configDir(tempDir)
                    .build();
        }
    }

    @Nested
    class ArgumentsValidation {

        @Test
        void shouldRejectWhenNotEnoughPositionalArguments() {
            // When/Then
            assertThatThrownBy(() -> deployNewInstanceByPropertiesFile())
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Expected 4 positional arguments, found 1");
        }

        @Test
        void shouldRejectWhenNeitherInstancePropertiesOrConfigDirSet() {
            // When/Then
            assertThatThrownBy(() -> deployNewInstanceByPropertiesFile("my-instance", "my-vpc", "my-subnets"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Either --instance-properties or --config-dir must be provided");
        }

        @Test
        void shouldRejectWhenBothInstancePropertiesAndConfigDirSet() {
            // When/Then
            assertThatThrownBy(() -> deployNewInstanceByPropertiesFile("my-instance", "my-vpc", "my-subnets",
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

    private void deployNewInstanceByPropertiesFile(String... args) throws Exception {
        deployNewInstance(true, args);
    }

    private void deployNewInstanceByConfigDir(String... args) throws Exception {
        deployNewInstance(false, args);
    }

    private void deployNewInstance(boolean isByPropFile, String... args) throws Exception {
        var arguments = DeployNewInstance.readArguments(CommandArgumentReader.parse(DeployNewInstance.USAGE,
                Stream.concat(Stream.of("scriptsDir"), Arrays.stream(args)).toArray(String[]::new)));
        var config = DeployNewInstance.loadAndUpdateConfiguration(arguments);

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

        if (isByPropFile) {
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
}
