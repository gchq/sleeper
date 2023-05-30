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
import org.junit.jupiter.api.Disabled;
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
import java.util.stream.Collectors;
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
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
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

    @Nested
    @DisplayName("Generate full example instance properties")
    class GenerateFullInstanceProperties {
        private final InstanceProperties properties = loadFullExampleInstanceProperties();

        @ParameterizedTest
        @ArgumentsSource(MandatoryInstancePropertyTemplateValues.class)
        void shouldSetMandatoryParameters(UserDefinedInstanceProperty property, String value) {
            assertThat(properties.get(property)).isEqualTo(value);
        }

        @Test
        @Disabled("TODO")
        void shouldExcludeSystemDefinedProperties() {
            String fullExampleString = loadFullExampleInstancePropertiesAsString();
            assertThat(fullExampleString)
                    .doesNotContain(SystemDefinedInstanceProperty.getAll().stream()
                            .map(SystemDefinedInstanceProperty::getPropertyName)
                            .collect(Collectors.toList()));
        }
    }

    private InstanceProperties loadFullExampleInstanceProperties() {
        InstanceProperties properties = new InstanceProperties();
        try {
            properties.load(tempDir.resolve("example/full/instance.properties"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return properties;
    }

    private String loadFullExampleInstancePropertiesAsString() {
        try {
            return Files.readString(tempDir.resolve("example/full/instance.properties"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
