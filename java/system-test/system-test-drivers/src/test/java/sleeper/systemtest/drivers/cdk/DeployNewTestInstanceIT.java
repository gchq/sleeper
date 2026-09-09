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
package sleeper.systemtest.drivers.cdk;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.clients.deploy.DeployInstanceRequest;
import sleeper.clients.deploy.DeployNewInstance;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.SaveLocalProperties;
import sleeper.core.properties.model.SleeperInternalCdkApp;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.util.cli.CommandArgumentReader;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.systemtest.drivers.cdk.DeployNewTestInstance.Arguments;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class DeployNewTestInstanceIT {
    // In-memory fakes standing in for the AWS-backed stores DeployNewInstance would otherwise create.
    InstanceProperties instanceProperties = new InstanceProperties();
    TableProperties tableProperties = new TableProperties(instanceProperties);
    InMemoryTableIndex tableIndex = new InMemoryTableIndex();
    TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStore(tableIndex);
    StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(instanceProperties,
            new InMemoryTransactionLogsPerTable());
    // Captures whatever gets deployed, instead of it going to real AWS/CDK.
    List<DeployInstanceRequest> deployRequests = new ArrayList<>();

    // Stands in for the scripts directory passed on the command line; holds the seeded deployAll templates.
    @TempDir
    private Path scriptsDir;
    // Stands in for a user-supplied --properties-file/--config-dir location.
    @TempDir
    private Path workDir;

    @BeforeEach
    void setUp() throws Exception {
        instanceProperties.set(FILE_SYSTEM, "test://");
        instanceProperties.setTags(Map.of("Project", "TemplateProject"));
        tableProperties.set(TABLE_NAME, "system-test");
        tableProperties.setSchema(createSchemaWithKey("key"));
        writeDefaultTemplates(instanceProperties, tableProperties);
    }

    private void writeDefaultTemplates(InstanceProperties instanceProperties, TableProperties tableProperties) throws Exception {
        // Seed the demo config templates where the default branch expects them (scripts/test/deployAll).
        // These will be the defaults when values are not populated by the user.
        Path deployAllDir = scriptsDir.resolve(DeployNewTestInstance.DEFAULT_CONFIG_DIRECTORY);
        Files.createDirectories(deployAllDir);
        Files.writeString(deployAllDir.resolve(DeployNewTestInstance.INSTANCE_PROPERTIES_FILE + ".template"), instanceProperties.saveAsString());
        Files.writeString(deployAllDir.resolve("table.properties.template"), tableProperties.saveAsString());
        Files.writeString(deployAllDir.resolve("schema.json.template"), new SchemaSerDe().toJson(tableProperties.getSchema()));
        Files.writeString(deployAllDir.resolve("tags.properties.template"), instanceProperties.getTagsPropertiesAsString());
    }

    @Nested
    @DisplayName("Default to the demo configuration when nothing is given")
    class Default {

        @Test
        void shouldLoadInstanceAndSystemTestTableFromDeployAllConfig() throws Exception {
            // When
            deployAndCaptureRequest();

            // Then the instance and table come from the deployAll config files
            instanceProperties.set(ID, "test-instance");
            instanceProperties.set(VPC_ID, "test-vpc");
            instanceProperties.set(SUBNETS, "test-subnet");
            // And the table properties object gains the table ID after deployment when the table is added
            tableProperties.set(TABLE_ID, tablePropertiesStore.loadByName("system-test").get(TABLE_ID));
            Path deployAllDir = scriptsDir.resolve(DeployNewTestInstance.DEFAULT_CONFIG_DIRECTORY);
            assertThat(deployRequests).containsExactly(DeployInstanceRequest.builder()
                    .instanceConfig(new SleeperInstanceConfiguration(instanceProperties, tableProperties))
                    .cdkCommand(CdkCommand.deployNew().withConfigurationDirectory(deployAllDir).toBuilder()
                            .instanceId("test-instance")
                            .vpcId("test-vpc")
                            .subnets("test-subnet")
                            .build())
                    .cdkApp(SleeperInternalCdkApp.DEMONSTRATION)
                    .build());
        }

        @Test
        void shouldSetInstanceIdVpcAndSubnetsFromPositionalArguments() throws Exception {
            // When
            SleeperInstanceConfiguration config = loadConfiguration();

            // Then
            assertThat(config.getInstanceProperties())
                    .extracting(properties -> properties.get(ID), properties -> properties.get(VPC_ID), properties -> properties.get(SUBNETS))
                    .containsExactly("test-instance", "test-vpc", "test-subnet");
        }

        @Test
        void shouldCreateRealConfigFilesFromTemplatesOnFirstUse() throws Exception {
            // When
            loadConfiguration();

            // Then the templates have been copied to their real config files, ready for a repeat deploy to reuse
            Path deployAllDir = scriptsDir.resolve(DeployNewTestInstance.DEFAULT_CONFIG_DIRECTORY);
            assertThat(deployAllDir.resolve(DeployNewTestInstance.INSTANCE_PROPERTIES_FILE)).exists();
            assertThat(deployAllDir.resolve("table.properties")).exists();
            assertThat(deployAllDir.resolve("schema.json")).exists();
            assertThat(deployAllDir.resolve("tags.properties")).exists();
        }

        @Test
        void shouldNotOverwriteAnExistingConfigFileOnSubsequentRuns() throws Exception {
            // Given a real config file already exists, customised by the user
            loadConfiguration();
            tableProperties.set(TABLE_NAME, "custom-table");
            Path tablePropertiesPath = scriptsDir.resolve(DeployNewTestInstance.DEFAULT_CONFIG_DIRECTORY).resolve("table.properties");
            Files.writeString(tablePropertiesPath, tableProperties.saveAsString());

            // When
            SleeperInstanceConfiguration config = loadConfiguration();

            // Then the customisation survived, it was not reset from the template
            assertThat(config.getTableProperties())
                    .extracting(properties -> properties.get(TABLE_NAME))
                    .containsExactly("custom-table");
        }
    }

    @Nested
    @DisplayName("Read only the instance when given --properties-file")
    class PropertiesFileGiven {

        @Test
        void shouldReadInstanceOnlyAndIgnoreSidecarTables() throws Exception {
            // Given an instance properties file with a table.properties sitting next to it
            Path propertiesFile = Files.writeString(workDir.resolve("instance.properties"), "sleeper.filesystem=from-file://");
            writeTableFiles(workDir, "sidecar-table");

            // When
            SleeperInstanceConfiguration config = loadConfiguration("--properties-file", propertiesFile.toString());

            // Then only the instance configuration is read; the sidecar table is not silently picked up (fixes #6593)
            assertThat(config.getInstanceProperties().get(FILE_SYSTEM)).isEqualTo("from-file://");
            assertThat(config.getTableProperties()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Read the whole directory when given --config-dir")
    class ConfigDirGiven {

        @Test
        void shouldDeployTablesDefinedInTheConfigurationDirectory() throws Exception {
            // Given
            Files.writeString(workDir.resolve("instance.properties"), "sleeper.filesystem=from-dir://");
            writeTableFiles(workDir, "my-table");

            // When
            SleeperInstanceConfiguration config = loadConfiguration("--config-dir", workDir.toString());

            // Then
            assertThat(config.getTableProperties())
                    .extracting(properties -> properties.get(TABLE_NAME))
                    .containsExactly("my-table");
        }

        @Test
        void shouldHaveNoTablesWhenConfigurationDirectoryHasNone() throws Exception {
            // Given
            Files.writeString(workDir.resolve("instance.properties"), "sleeper.filesystem=from-dir://");

            // When
            SleeperInstanceConfiguration config = loadConfiguration("--config-dir", workDir.toString());

            // Then an empty config directory means no tables are deployed
            assertThat(config.getTableProperties()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Apply optional flags")
    class OptionalFlags {

        @Test
        void shouldSetDeployPausedFlag() {
            // When
            Arguments args = readArguments("--paused");

            // Then
            assertThat(args.deployPaused()).isTrue();
        }

        @Test
        void shouldNotSetDeployPausedFlagByDefault() {
            // When
            Arguments args = readArguments();

            // Then
            assertThat(args.deployPaused()).isFalse();
        }
    }

    @Nested
    @DisplayName("Validate arguments")
    class ArgumentsValidation {

        @Test
        void shouldRejectWhenBothPropertiesFileAndConfigDirSet() {
            // When / Then
            assertThatThrownBy(() -> readArguments("--properties-file", "someFile", "--config-dir", "someDir"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Cannot use both --properties-file and --config-dir");
        }
    }

    @Nested
    @DisplayName("Deploy the loaded configuration")
    class Deploy {

        @Test
        void shouldDeployTablesFromConfigurationDirectory() throws Exception {
            // Given
            instanceProperties.set(FILE_SYSTEM, "from-dir://");
            tableProperties.set(TABLE_NAME, "my-table");
            writeToFile(workDir.resolve("instance.properties"), instanceProperties, tableProperties);

            // When
            deployAndCaptureRequest("--config-dir", workDir.toString());

            // Then it forwards the tables defined in the directory
            assertThat(deployRequests).singleElement().satisfies(request -> assertThat(request.getInstanceConfig().getTableProperties())
                    .extracting(properties -> properties.get(TABLE_NAME))
                    .containsExactly("my-table"));
        }

        @Test
        void shouldDeployPausedWhenFlagIsSet() throws Exception {
            // When
            deployAndCaptureRequest("--paused");

            // Then the --paused flag reaches the CDK command, not just the parsed arguments
            assertThat(deployRequests).singleElement().satisfies(request -> assertThat(request.getCdkCommand().arguments()).contains("deployPaused=true"));
        }
    }

    // Runs the real deploy() seam with in-memory fakes standing in for AWS.
    private void deployAndCaptureRequest(String... options) throws Exception {
        DeployNewTestInstance.deploy(readArguments(options),
                request -> deployRequests.add(request),
                new DeployNewInstance.StoreFactory() {
                    public TablePropertiesStore createTableStore(InstanceProperties p) {
                        return tablePropertiesStore;
                    }

                    public StateStoreProvider createStateStore(InstanceProperties p) {
                        return stateStoreProvider;
                    }
                },
                instanceId -> {
                    instanceProperties.set(ID, instanceId);
                    return instanceProperties;
                });
    }

    private SleeperInstanceConfiguration loadConfiguration(String... options) throws IOException {
        return DeployNewTestInstance.loadConfiguration(readArguments(options));
    }

    // Prepends the fixed positional arguments (scriptsDir, instance ID, VPC, subnets) to whatever options a test passes.
    private Arguments readArguments(String... options) {
        return DeployNewTestInstance.readArguments(CommandArgumentReader.parse(DeployNewTestInstance.USAGE,
                Stream.concat(
                        Stream.of(scriptsDir.toString(), "test-instance", "test-vpc", "test-subnet"),
                        Arrays.stream(options))
                        .toArray(String[]::new)));
    }

    private void writeToFile(Path file, InstanceProperties instanceProperties, TableProperties... tableProperties) throws Exception {
        SaveLocalProperties.saveToFile(file, instanceProperties, Stream.of(tableProperties));
    }

    // Writes a table.properties + schema.json pair, as a --config-dir or a sidecar next to an instance.properties file.
    private void writeTableFiles(Path directory, String tableName) throws IOException {
        Files.writeString(directory.resolve("table.properties"), "sleeper.table.name=" + tableName);
        Files.writeString(directory.resolve("schema.json"), new SchemaSerDe().toJson(createSchemaWithKey("key")));
    }
}
