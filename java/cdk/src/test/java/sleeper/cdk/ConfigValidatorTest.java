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
package sleeper.cdk;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.InstanceProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;

class ConfigValidatorTest {

    @TempDir
    public Path temporaryFolder;

    private ConfigValidator configValidator;
    private final InstanceProperties instanceProperties = new InstanceProperties();

    @BeforeEach
    public void setUp() {
        configValidator = new ConfigValidator();
    }

    @Test
    void shouldNotThrowAnErrorWithValidConfiguration() throws IOException {
        // Given
        instanceProperties.set(ID, "valid-id");
        setupTablesPropertiesFile("example-valid-table", "sleeper.statestore.dynamodb.DynamoDBStateStore");

        // When / Then
        assertThatCode(this::validate)
                .doesNotThrowAnyException();
    }

    @Test
    void shouldThrowAnErrorWhenTableNameIsNotValid() throws IOException {
        // Given
        instanceProperties.set(ID, "valid-id");
        setupTablesPropertiesFile("example--invalid-name-tab$$-le", "sleeper.statestore.dynamodb.DynamoDBStateStore");

        // When / Then
        assertThatThrownBy(this::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Sleeper table bucket name is illegal: sleeper-valid-id-table-example--invalid-name-tab$$-le");
    }

    @Test
    void shouldThrowAnErrorWithAnInvalidSleeperId() {
        // Given
        instanceProperties.set(ID, "aa$$aa");

        // When / Then
        assertThatThrownBy(this::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Sleeper instance id is illegal: aa$$aa");
    }

    private void validate() throws IOException {
        Path instancePropertiesPath = temporaryFolder.resolve("instance.properties");
        Files.writeString(instancePropertiesPath, instanceProperties.saveAsString());
        configValidator.validate(instanceProperties, instancePropertiesPath);
    }

    private void setupTablesPropertiesFile(String tableName, String stateStore) throws IOException {
        String tableSchema = "{\n" +
                "  \"rowKeyFields\": [ \n" +
                "    {\n" +
                "      \"name\": \"key\",\n" +
                "      \"type\": \"StringType\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"sortKeyFields\": [\n" +
                "    {\n" +
                "      \"name\": \"timestamp\",\n" +
                "      \"type\": \"LongType\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"valueFields\": [\n" +
                "    {\n" +
                "      \"name\": \"value\",\n" +
                "      \"type\": \"StringType\"\n" +
                "    }\n" +
                "  ]\n" +
                "}\n";

        Files.writeString(temporaryFolder.resolve("schema.json"), tableSchema);

        String tableConfiguration = "" +
                String.format("sleeper.table.name=%s\n", tableName) +
                String.format("sleeper.table.statestore.classname=%s\n", stateStore);

        Files.writeString(temporaryFolder.resolve("table.properties"), tableConfiguration);
    }
}
