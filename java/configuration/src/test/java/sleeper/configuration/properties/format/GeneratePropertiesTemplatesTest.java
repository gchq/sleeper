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

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import sleeper.configuration.properties.UserDefinedInstanceProperty;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;

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
                    Arguments.of(SUBNET, "subnet-abcdefgh")
            );
        }
    }

    static class SystemDefinedInstanceProperties implements ArgumentsProvider {
        @Override
        public Stream<Arguments> provideArguments(ExtensionContext context) {
            return SystemDefinedInstanceProperty.getAll().stream()
                    .map(Arguments::of);
        }
    }

    @Nested
    @DisplayName("Generate full example instance properties")
    class GenerateFullInstanceProperties {
        private final String propertiesString = loadFullExampleInstancePropertiesAsString();

        @ParameterizedTest
        @ArgumentsSource(MandatoryInstancePropertyTemplateValues.class)
        void shouldSetMandatoryParameters(UserDefinedInstanceProperty property, String value) {
            assertThat(instancePropertiesFromString(propertiesString).get(property)).isEqualTo(value);
        }

        @ParameterizedTest
        @ArgumentsSource(SystemDefinedInstanceProperties.class)
        void shouldExcludeSystemDefinedProperties(SystemDefinedInstanceProperty property) {
            assertThat(propertiesString)
                    .doesNotContain(property.getPropertyName() + "=");
        }

        @Test
        void shouldNotCommentOutUnsetParameter() {
            assertThat(propertiesString)
                    .contains(System.lineSeparator() +
                            "sleeper.userjars=" +
                            System.lineSeparator());
        }

        @Test
        void shouldNotCommentOutParameterSetToDefaultValue() {
            assertThat(propertiesString)
                    .contains(System.lineSeparator() +
                            "sleeper.stack.tag.name=DeploymentStack" +
                            System.lineSeparator());
        }
    }

    private String loadFullExampleInstancePropertiesAsString() {
        try {
            return Files.readString(tempDir.resolve("example/full/instance.properties"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private InstanceProperties instancePropertiesFromString(String propertiesString) {
        InstanceProperties properties = new InstanceProperties();
        try {
            properties.loadFromString(propertiesString);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return properties;
    }
}
