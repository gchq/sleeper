/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.configuration.properties.format;

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

import sleeper.configuration.properties.deploy.GeneratePropertiesTemplates;
import sleeper.configuration.properties.instance.CdkDefinedInstanceProperty;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TableProperties;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.regex.Pattern.DOTALL;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.instance.CommonProperty.VPC_ID;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.SCHEMA;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.PropertiesUtils.loadProperties;

class GeneratePropertiesTemplatesTest {

    @TempDir
    private static Path tempDir;

    @BeforeAll
    static void setUp() throws Exception {
        GeneratePropertiesTemplates.fromRepositoryPath(tempDir);
    }

    static class MandatoryInstancePropertyTemplateValues implements ArgumentsProvider {
        @Override
        public Stream<Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    Arguments.of(ID, "full-example"),
                    Arguments.of(JARS_BUCKET, "the name of the bucket containing your jars, e.g. sleeper-<insert-unique-name-here>-jars"),
                    Arguments.of(ACCOUNT, "1234567890"),
                    Arguments.of(REGION, "eu-west-2"),
                    Arguments.of(VPC_ID, "1234567890"),
                    Arguments.of(SUBNETS, "subnet-abcdefgh"));
        }
    }

    static class SystemDefinedInstanceProperties implements ArgumentsProvider {
        @Override
        public Stream<Arguments> provideArguments(ExtensionContext context) {
            return CdkDefinedInstanceProperty.getAll().stream()
                    .map(Arguments::of);
        }
    }

    @Nested
    @DisplayName("Generate full example instance properties")
    class GenerateFullInstanceProperties {
        private final String propertiesString = loadFileAsString("example/full/instance.properties");

        @ParameterizedTest
        @ArgumentsSource(MandatoryInstancePropertyTemplateValues.class)
        void shouldSetMandatoryParameters(UserDefinedInstanceProperty property, String value) {
            assertThat(instancePropertiesFromString(propertiesString)
                    .get(property))
                    .isEqualTo(value);
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
        void shouldWriteDefaultValueForUnsetParameter() {
            assertThat(propertiesString)
                    .contains(System.lineSeparator() +
                            "sleeper.stack.tag.name=DeploymentStack" +
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
        void shouldGenerateValidTableProperties() {
            assertThat(tablePropertiesFromString(propertiesString)
                    .get(TABLE_NAME))
                    .isEqualTo("example-table");
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
        void shouldGenerateValidInstanceProperties() {
            assertThat(instancePropertiesFromString(propertiesString)
                    .get(ID))
                    .isEqualTo("basic-example");
        }
    }

    @Nested
    @DisplayName("Generate basic example table properties")
    class GenerateBasicTableProperties {
        private final String propertiesString = loadFileAsString("example/basic/table.properties");

        @Test
        void shouldGenerateValidTableProperties() {
            assertThat(tablePropertiesFromString(propertiesString)
                    .get(TABLE_NAME))
                    .isEqualTo("example-table");
        }

        @Test
        void shouldNotIncludePropertyDefaultedFromNonMandatoryInstanceProperty() {
            assertThat(propertiesString)
                    .doesNotContain("sleeper.table.compression.codec");
        }

        @Test
        void shouldIncludeSpecificallySetProperty() {
            assertThat(tablePropertiesFromString(propertiesString)
                    .get(ITERATOR_CLASS_NAME))
                    .isEqualTo("sleeper.core.iterator.impl.AgeOffIterator");
        }
    }

    @Nested
    @DisplayName("Generate instance properties template")
    class GenerateInstancePropertiesTemplate {
        private final String propertiesString = loadFileAsString("scripts/templates/instanceproperties.template");

        @Test
        void shouldGenerateValidInstanceProperties() {
            assertThat(instancePropertiesFromString(propertiesString)
                    .get(ID))
                    .isEqualTo("changeme");
        }

        @Test
        void shouldGenerateHeadersForTemplatedPropertiesAndDefaultedProperties() {
            assertThat(propertiesString).containsSubsequence(
                    "# Template Values #",
                    "changeme",
                    "# Default Values #");
        }

        @Test
        void shouldNotIncludeAnyTemplatedPropertiesUnderDefaultValues() {
            assertThat(propertiesString).doesNotMatch(
                    Pattern.compile(".*# Default Values #.+changeme.*", DOTALL));
        }
    }

    @Nested
    @DisplayName("Generate table properties template")
    class GenerateTablePropertiesTemplate {
        private final String propertiesString = loadFileAsString("scripts/templates/tableproperties.template");

        @Test
        void shouldGenerateValidTableProperties() {
            assertThat(tablePropertiesFromString(propertiesString)
                    .get(TABLE_NAME))
                    .isEqualTo("changeme");
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
        return InstanceProperties.createAndValidate(loadProperties(propertiesString));
    }

    private TableProperties tablePropertiesFromString(String propertiesString) {
        return new TableProperties(new InstanceProperties(), loadProperties(propertiesString));
    }
}
