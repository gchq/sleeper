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
package sleeper.clients.deploy.documentation;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.instance.UserDefinedInstanceProperty;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static sleeper.core.properties.PropertiesUtils.loadProperties;
import static sleeper.core.properties.instance.CommonProperty.ARTEFACTS_DEPLOYMENT_ID;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.table.TableProperty.SCHEMA;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

class GeneratePropertiesTemplatesTest {

    @TempDir
    private static Path tempDir;

    @BeforeAll
    static void setUp() throws Exception {
        GeneratePropertiesTemplates.createTemplates(tempDir);
    }

    static class SystemDefinedInstanceProperties implements ArgumentsProvider {
        @Override
        public Stream<Arguments> provideArguments(ExtensionContext context) {
            return CdkDefinedInstanceProperty.getAll().stream()
                    .map(Arguments::of);
        }
    }

    static class DeploymentInstancePropertyValues implements ArgumentsProvider {
        @Override
        public Stream<Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    Arguments.of(ID),
                    Arguments.of(VPC_ID),
                    Arguments.of(SUBNETS));
        }
    }

    @Nested
    @DisplayName("Generate full example instance properties")
    class GenerateFullInstanceProperties {
        private final String propertiesString = loadFileAsString("example/full/instance.properties");

        @Test
        void shouldGenerateValidInstanceProperties() {
            // When
            InstanceProperties validProperties = instancePropertiesFromString(propertiesString);
            validProperties.set(ID, "test-id");
            validProperties.set(VPC_ID, "test-vpc");
            validProperties.set(SUBNETS, "test-subnets");

            // Then
            assertThatCode(validProperties::validate).doesNotThrowAnyException();
        }

        @ParameterizedTest
        @ArgumentsSource(DeploymentInstancePropertyValues.class)
        void shouldNotSetDeploymentProperties(UserDefinedInstanceProperty property) {
            assertThat(instancePropertiesFromString(propertiesString)
                    .get(property)).isNull();
        }

        @ParameterizedTest
        @ArgumentsSource(SystemDefinedInstanceProperties.class)
        void shouldExcludeSystemDefinedProperties(CdkDefinedInstanceProperty property) {
            assertThat(propertiesString)
                    .doesNotContain(property.getPropertyName() + "=");
        }

        @Test
        void shouldCommentOutUnsetParameterWithNoDefaultValue() {
            assertThat(propertiesString)
                    .contains(System.lineSeparator() +
                            "# sleeper.userjars=" +
                            System.lineSeparator());
        }

        @Test
        void shouldWriteCommentedOutDefaultValueForPropertyWithDefault() {
            assertThat(propertiesString)
                    .contains(System.lineSeparator() +
                            "# sleeper.stack.tag.name=DeploymentStack" +
                            System.lineSeparator());
        }

        @Test
        void shouldExcludeTagsProperty() {
            assertThat(propertiesString)
                    .doesNotContain("sleeper.tags=");
        }
    }

    @Nested
    @DisplayName("Generate full example table properties")
    class GenerateFullTableProperties {
        private final String propertiesString = loadFileAsString("example/full/table.properties");

        @Test
        void shouldGenerateValidTablePropertiesIfSchemaIsAdded() {
            // Given
            TableProperties tableProperties = tablePropertiesFromString(propertiesString);
            assertThat(TableProperty.getAll().stream()
                    .allMatch(presentProperty -> tableProperties.isSet(presentProperty) == Boolean.FALSE))
                    .isTrue();

            // When
            tableProperties.setSchema(createSchemaWithKey("key"));
            setMandatoryTableProperties(tableProperties);

            // Then
            assertThatCode(tableProperties::validate)
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldNotSetSchemaInFile() {
            assertThat(propertiesString)
                    .doesNotContain(SCHEMA.getPropertyName());
        }
    }

    @Nested
    @DisplayName("Generate basic example instance properties")
    class GenerateBasicInstanceProperties {
        private final String propertiesString = loadFileAsString("example/basic/instance.properties");

        @Test
        void shouldGenerateEmptyInstanceProperties() {
            assertThat(instancePropertiesFromString(propertiesString)).isEqualTo(new InstanceProperties());
        }
    }

    @Nested
    @DisplayName("Generate basic example table properties")
    class GenerateBasicTableProperties {
        private final String propertiesString = loadFileAsString("example/basic/table.properties");

        @Test
        void shouldGenerateValidTablePropertiesIfSchemaIsAdded() {
            // When
            TableProperties tableProperties = tablePropertiesFromString(propertiesString);
            tableProperties.setSchema(createSchemaWithKey("key"));

            setMandatoryTableProperties(tableProperties);

            // Then
            assertThatCode(tableProperties::validate)
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldNotIncludePropertyDefaultedFromNonMandatoryInstanceProperty() {
            assertThat(propertiesString)
                    .doesNotContain("sleeper.table.parquet.compression.codec");
        }
    }

    @Nested
    @DisplayName("Generate instance properties template")
    class GenerateInstancePropertiesTemplate {
        private final String propertiesString = loadFileAsString("scripts/templates/instanceproperties.template");

        @Test
        void shouldGenerateEmptyInstanceProperties() {
            // When
            InstanceProperties instanceProperties = instancePropertiesFromString(propertiesString);

            // Then
            assertThat(instanceProperties).isEqualTo(new InstanceProperties());
        }

        @Test
        void shouldNotSetValuesForAnyPropertyWithDefault() {
            // When
            InstanceProperties instanceProperties = instancePropertiesFromString(propertiesString);

            // Then
            assertThat(instancePropertiesWithDefaultValues())
                    .allSatisfy(property -> assertThat(instanceProperties.isSet(property)).isFalse());
        }
    }

    @Nested
    @DisplayName("Generate table properties template")
    class GenerateTablePropertiesTemplate {
        private final String propertiesString = loadFileAsString("scripts/templates/tableproperties.template");

        @Test
        void shouldGenerateValidTablePropertiesIfSchemaIsAdded() {
            // Given
            TableProperties tableProperties = tablePropertiesFromString(propertiesString);

            // When
            tableProperties.setSchema(createSchemaWithKey("key"));
            setMandatoryTableProperties(tableProperties);

            // Then
            assertThatCode(tableProperties::validate)
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldNotSetValuesForAnyPropertyWithDefault() {
            // When
            TableProperties tableProperties = tablePropertiesFromString(propertiesString);

            // Then
            assertThat(tablePropertiesWithDefaultValues())
                    .allSatisfy(property -> assertThat(tableProperties.isSet(property)).isFalse());
        }
    }

    private String loadFileAsString(String path) {
        try {
            return Files.readString(tempDir.resolve(path));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private InstanceProperties instancePropertiesFromString(String propertiesString) {
        return InstanceProperties.createWithoutValidation(loadProperties(propertiesString));
    }

    private TableProperties tablePropertiesFromString(String propertiesString) {
        return new TableProperties(new InstanceProperties(), loadProperties(propertiesString));
    }

    private Stream<InstanceProperty> instancePropertiesWithDefaultValues() {
        // List of exemptions, where they have a default property but neither it or the default are set
        List<InstanceProperty> exemptions = List.of(ARTEFACTS_DEPLOYMENT_ID);

        return InstanceProperty.getAll().stream()
                .filter(property -> (property.getDefaultValue() != null || property.getDefaultProperty() != null) && !exemptions.contains(property));
    }

    private Stream<TableProperty> tablePropertiesWithDefaultValues() {
        return TableProperty.getAll().stream()
                .filter(property -> property.getDefaultValue() != null || property.getDefaultProperty() != null);
    }

    private void setMandatoryTableProperties(TableProperties tableProperties) {
        tableProperties.set(TABLE_ID, "test-table-id");
        tableProperties.set(TABLE_NAME, "test-table-name");
    }
}
