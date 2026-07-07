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

import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.SleeperInternalCdkApp;
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
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstancePropertiesWithId;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class DeployNewInstanceTest {
    InstanceProperties instanceProperties = createTestInstancePropertiesWithId("my-instance");
    Schema schema = createSchemaWithKey("key");
    InMemoryTableIndex tableIndex = new InMemoryTableIndex();
    TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStore(tableIndex);
    StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(instanceProperties, new InMemoryTransactionLogsPerTable());
    Map<String, InstanceProperties> instanceIdToProperties = new HashMap<>();
    Map<Path, String> pathToString = new HashMap<>();

    @BeforeEach
    void setUp() {
        instanceIdToProperties.put("my-instance", instanceProperties);
        saveSchemaFile("./schema.json", schema);
        saveFile("./table.properties", "sleeper.table.name=file-table\n");
    }

    //TODO test deploy method

    @Nested
    class ArgumentsValidation {

        @Test
        void shouldRejectWhenNotEnoughPositionalArguments() {
            //When/Then
            assertThatThrownBy(() -> deployNewInstance())
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Expected 4 positional arguments, found 0");
        }

        @Test
        void shouldRejectWhenNeitherInstancePropertiesOrConfigDirSet() {
            //When/Then
            assertThatThrownBy(() -> deployNewInstance("scriptsDir", "my-instance", "my-vpc", "my-subnets"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Either --instance-properties or --config-dir must be provided");
        }

        @Test
        void shouldRejectWhenBothInstancePropertiesAndConfigDirSet() {
            //When/Then
            assertThatThrownBy(() -> deployNewInstance("scriptsDir", "my-instance", "my-vpc", "my-subnets", "--instance-properties", "someFile", "--config-dir", "someDir"))
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
        SleeperInstanceConfiguration config = SleeperInstanceConfiguration.withNoTables(instanceProperties);
        new DeployNewInstance(
                request -> {
                },           // no-op stub — no real CDK/S3/ECR
                this::loadInstanceProperties,
                new DeployNewInstance.StoreFactory() {
                    public TablePropertiesStore createTableStore(InstanceProperties p) {
                        return tablePropertiesStore;
                    }

                    public StateStoreProvider createStateStore(InstanceProperties p) {
                        return stateStoreProvider;
                    }
                },
                config, SleeperInternalCdkApp.STANDARD,
                arguments.ignoreTableFiles(), arguments.deployPaused()).deploy();
    }

    private void saveSchemaFile(String path, Schema schema) {
        pathToString.put(Path.of(path), new SchemaSerDe().toJson(schema));
    }

    private void saveFile(String path, String content) {
        pathToString.put(Path.of(path), content);
    }

    private InstanceProperties loadInstanceProperties(String instanceId) {
        return Optional.ofNullable(instanceIdToProperties.get(instanceId))
                .orElseThrow();
    }

    private String readFile(Path path) throws IOException {
        try {
            return Optional.ofNullable(pathToString.get(path)).orElseThrow();
        } catch (NoSuchElementException e) {
            throw new IOException(e);
        }
    }
}
