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
import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.SleeperInternalCdkApp;
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
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class DeployNewTestInstanceIT {
    // In-memory fakes standing in for the AWS-backed stores DeployNewInstance would otherwise create.
    InstanceProperties deployedProperties = new InstanceProperties();
    InMemoryTableIndex tableIndex = new InMemoryTableIndex();
    TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStore(tableIndex);
    StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(deployedProperties,
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
    void setUp() throws IOException {
        writeDemoConfigTemplates();
    }

    @Nested
    @DisplayName("Default to the demo configuration when nothing is given")
    class Default {

        @Test
        void shouldLoadInstanceAndSystemTestTableFromDeployAllConfig() throws Exception {
            // When
            SleeperInstanceConfiguration config = loadConfiguration();

            // Then the instance and table come from the deployAll config files
            assertThat(config.getInstanceProperties().get(FILE_SYSTEM)).isEqualTo("test://");
            assertThat(config.getTableProperties())
                    .extracting(properties -> properties.get(TABLE_NAME))
                    .containsExactly("system-test");
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
        void shouldDeployDemoConfigurationAsDemonstrationInstance() throws Exception {
            // When
            DeployInstanceRequest request = deployAndCaptureRequest();

            // Then it deploys as the demonstration app, with the instance and system-test table it loaded
            assertThat(request.getCdkApp()).isEqualTo(SleeperInternalCdkApp.DEMONSTRATION);
            assertThat(request.getInstanceConfig().getInstanceProperties())
                    .extracting(properties -> properties.get(ID), properties -> properties.get(VPC_ID),
                            properties -> properties.get(SUBNETS), properties -> properties.get(FILE_SYSTEM))
                    .containsExactly("test-instance", "test-vpc", "test-subnet", "test://");
            assertThat(request.getInstanceConfig().getTableProperties())
                    .extracting(properties -> properties.get(TABLE_NAME))
                    .containsExactly("system-test");
        }

        @Test
        void shouldDeployTablesFromConfigurationDirectory() throws Exception {
            // Given
            Files.writeString(workDir.resolve("instance.properties"), "sleeper.filesystem=from-dir://");
            writeTableFiles(workDir, "my-table");

            // When
            DeployInstanceRequest request = deployAndCaptureRequest("--config-dir", workDir.toString());

            // Then it forwards the tables defined in the directory
            assertThat(request.getInstanceConfig().getTableProperties())
                    .extracting(properties -> properties.get(TABLE_NAME))
                    .containsExactly("my-table");
        }

        @Test
        void shouldDeployPausedWhenFlagIsSet() throws Exception {
            // When
            DeployInstanceRequest request = deployAndCaptureRequest("--paused");

            // Then the --paused flag reaches the CDK command, not just the parsed arguments
            assertThat(request.getCdkCommand().arguments()).contains("deployPaused=true");
        }
    }

    // Runs the real deploy() seam with in-memory fakes standing in for AWS, and returns what got captured.
    private DeployInstanceRequest deployAndCaptureRequest(String... options) throws Exception {
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
                    deployedProperties.set(ID, instanceId);
                    return deployedProperties;
                });
        return deployRequests.get(0);
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

    // Seed the demo config templates where the default branch expects them (scripts/test/deployAll).
    private void writeDemoConfigTemplates() throws IOException {
        Path deployAllDir = scriptsDir.resolve(DeployNewTestInstance.DEFAULT_CONFIG_DIRECTORY);
        Files.createDirectories(deployAllDir);
        // Instance config template - read for the default instance properties.
        Files.writeString(deployAllDir.resolve(DeployNewTestInstance.INSTANCE_PROPERTIES_FILE + ".template"), "sleeper.filesystem=test://");
        // Table config template - gives the demo's "system-test" table its name.
        Files.writeString(deployAllDir.resolve("table.properties.template"), "sleeper.table.name=system-test");
        // Schema template - loaded automatically from beside table.properties.
        Files.writeString(deployAllDir.resolve("schema.json.template"), new SchemaSerDe().toJson(createSchemaWithKey("key")));
        // Tags template - applied to the demo instance.
        Files.writeString(deployAllDir.resolve("tags.properties.template"), "Project=TestProject");
    }

    // Writes a table.properties + schema.json pair, as a --config-dir or a sidecar next to an instance.properties file.
    private void writeTableFiles(Path directory, String tableName) throws IOException {
        Files.writeString(directory.resolve("table.properties"), "sleeper.table.name=" + tableName);
        Files.writeString(directory.resolve("schema.json"), new SchemaSerDe().toJson(createSchemaWithKey("key")));
    }
}
