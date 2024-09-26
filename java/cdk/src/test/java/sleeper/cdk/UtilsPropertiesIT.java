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
package sleeper.cdk;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.cdk.util.Utils;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.SaveLocalProperties;
import sleeper.core.properties.table.TableProperties;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.cdk.UtilsTestHelper.createUserDefinedInstanceProperties;
import static sleeper.cdk.UtilsTestHelper.createUserDefinedTableProperties;
import static sleeper.core.SleeperVersion.getVersion;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ID;

class UtilsPropertiesIT {

    @TempDir
    private Path tempDir;

    @Nested
    @DisplayName("Load user defined properties from local configuration")
    class LoadUserDefinedProperties {

        @Test
        void shouldLoadValidInstancePropertiesFromFile() throws IOException {
            // Given
            InstanceProperties properties = createUserDefinedInstanceProperties();
            SaveLocalProperties.saveToDirectory(tempDir, properties, Stream.empty());

            // When / Then
            properties.set(VERSION, getVersion());
            assertThat(loadInstanceProperties(cdkContextWithPropertiesFile()))
                    .isEqualTo(properties);
        }

        @Test
        void shouldLoadValidTablePropertiesFromFile() throws IOException {
            // Given
            InstanceProperties instanceProperties = createUserDefinedInstanceProperties();
            TableProperties properties = createUserDefinedTableProperties(instanceProperties);
            SaveLocalProperties.saveToDirectory(tempDir, instanceProperties, Stream.of(properties));

            // When / Then
            assertThat(Utils.getAllTableProperties(instanceProperties, cdkContextWithPropertiesFile()))
                    .containsExactly(properties);
        }

        @Test
        void shouldClearSystemDefinedPropertiesWhenInstancePropertiesAreLoaded() throws IOException {
            // Given
            InstanceProperties properties = createUserDefinedInstanceProperties();
            properties.set(BULK_IMPORT_BUCKET, "test-bulk-import-bucket");
            SaveLocalProperties.saveToDirectory(tempDir, properties, Stream.empty());

            // When
            InstanceProperties loaded = loadInstanceProperties(cdkContextWithPropertiesFile());

            // Then
            assertThat(loaded.get(BULK_IMPORT_BUCKET)).isNull();
        }

        @Test
        void shouldSetVersionWhenInstancePropertiesAreLoaded() throws IOException {
            // Given
            InstanceProperties properties = createUserDefinedInstanceProperties();
            SaveLocalProperties.saveToDirectory(tempDir, properties, Stream.empty());

            // When
            InstanceProperties loaded = loadInstanceProperties(cdkContextWithPropertiesFile());

            // Then
            assertThat(loaded.get(VERSION))
                    .matches("\\d+\\.\\d+\\.\\d+(-SNAPSHOT)?");
        }
    }

    @Nested
    @DisplayName("Ensure configuration will result in valid AWS resource names")
    class ValidateResourceNames {

        @Test
        void shouldFailWhenInstanceIdIsNotAValidBucketName() throws IOException {
            // Given
            InstanceProperties instanceProperties = createUserDefinedInstanceProperties();
            instanceProperties.set(ID, "aa$$aa");
            SaveLocalProperties.saveToDirectory(tempDir, instanceProperties, Stream.empty());

            // When / Then
            Function<String, String> context = cdkContextWithPropertiesFile();
            assertThatThrownBy(() -> loadInstanceProperties(context))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Sleeper instance ID is not valid as part of an S3 bucket name: aa$$aa");
        }
    }

    private InstanceProperties loadInstanceProperties(Function<String, String> context) {
        return Utils.loadInstanceProperties(InstanceProperties::createWithoutValidation, context);
    }

    private Function<String, String> cdkContextWithPropertiesFile() {
        return Map.of("propertiesfile", tempDir.resolve("instance.properties").toString())::get;
    }
}
