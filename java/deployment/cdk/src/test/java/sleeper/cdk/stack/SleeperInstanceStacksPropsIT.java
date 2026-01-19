/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.cdk.stack;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.Stack;

import sleeper.cdk.SleeperInstanceProps;
import sleeper.cdk.networking.SleeperNetworking;
import sleeper.cdk.util.MismatchedVersionException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.SaveLocalProperties;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.SleeperVersion.getVersion;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACCOUNT;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;

class SleeperInstanceStacksPropsIT {

    private final String account = "test-account";
    private final String region = "test-region";
    private final String vpcId = "vpc-12345";
    private final List<String> subnetIds = List.of("subnet-12345");
    private final Map<String, String> cdkContext = new HashMap<>();

    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp() {
        cdkContext.put("propertiesfile", tempDir.resolve("instance.properties").toString());
    }

    @Nested
    @DisplayName("Load user defined properties from local configuration")
    class LoadUserDefinedProperties {

        @Test
        void shouldLoadValidInstancePropertiesFromFile() throws IOException {
            // Given
            InstanceProperties properties = createUserDefinedInstanceProperties();
            SaveLocalProperties.saveToDirectory(tempDir, properties, Stream.empty());

            // When
            InstanceProperties loaded = loadInstanceProperties();

            // Then
            Properties tagsProperties = properties.getTagsProperties();
            tagsProperties.setProperty("InstanceID", properties.get(ID));
            properties.loadTags(tagsProperties);
            properties.set(VERSION, getVersion());
            properties.set(ACCOUNT, account);
            properties.set(REGION, region);
            properties.set(VPC_ID, vpcId);
            properties.setList(SUBNETS, subnetIds);

            assertThat(loaded).isEqualTo(properties);
        }

        @Test
        void shouldClearSystemDefinedPropertiesWhenInstancePropertiesAreLoaded() throws IOException {
            // Given
            InstanceProperties properties = createUserDefinedInstanceProperties();
            properties.set(BULK_IMPORT_BUCKET, "test-bulk-import-bucket");
            SaveLocalProperties.saveToDirectory(tempDir, properties, Stream.empty());

            // When
            InstanceProperties loaded = loadInstanceProperties();

            // Then
            assertThat(loaded.get(BULK_IMPORT_BUCKET)).isNull();
        }

        @Test
        void shouldSetVersionWhenInstancePropertiesAreLoaded() throws IOException {
            // Given
            InstanceProperties properties = createUserDefinedInstanceProperties();
            SaveLocalProperties.saveToDirectory(tempDir, properties, Stream.empty());

            // When
            InstanceProperties loaded = loadInstanceProperties();

            // Then
            assertThat(loaded.get(VERSION))
                    .matches("\\d+\\.\\d+\\.\\d+(-SNAPSHOT)?");
        }

        @Test
        void shouldSetTagPropertiesWhenInstancePropertiesAreLoaded() throws IOException {
            // Given
            InstanceProperties properties = createUserDefinedInstanceProperties();
            SaveLocalProperties.saveToDirectory(tempDir, properties, Stream.empty());

            // When
            InstanceProperties loaded = loadInstanceProperties();

            // Then
            assertThat(loaded.getTags().get("InstanceID"))
                    .matches("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}$");
        }
    }

    @Nested
    @DisplayName("Set properties from context variables")
    class SetPropertiesFromContextVariables {

        @Test
        void shouldSetInstanceIdInContextVariable() throws Exception {
            // Given
            InstanceProperties properties = createUserDefinedInstanceProperties();
            SaveLocalProperties.saveToDirectory(tempDir, properties, Stream.empty());
            cdkContext.put("id", "test-instance");

            // When
            InstanceProperties loaded = loadInstanceProperties();

            // Then
            assertThat(loaded.get(ID)).isEqualTo("test-instance");
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
            assertThatThrownBy(() -> readProps())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Sleeper instance ID is not valid as part of an S3 bucket name: aa$$aa");
        }
    }

    @Nested
    @DisplayName("Validate version")
    class ValidateVersion {

        @Test
        void shouldSucceedVersionCheckWhenLocalVersionMatchesDeployedVersion() throws IOException {
            // Given
            InstanceProperties instanceProperties = createInstancePropertiesWithVersion(getVersion());
            SaveLocalProperties.saveToDirectory(tempDir, instanceProperties, Stream.empty());

            // When / Then
            assertThatCode(() -> readProps())
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldFailVersionCheckWhenLocalVersionDoesNotMatchDeployedVersion() throws IOException {
            // Given
            InstanceProperties instanceProperties = createInstancePropertiesWithVersion("0.14.0-SNAPSHOT");
            SaveLocalProperties.saveToDirectory(tempDir, instanceProperties, Stream.empty());

            // When / Then
            assertThatThrownBy(() -> readProps())
                    .isInstanceOf(MismatchedVersionException.class)
                    .hasMessage("Local version " + getVersion() + " does not match deployed version 0.14.0-SNAPSHOT. " +
                            "Please upgrade/downgrade to make these match");
        }

        @Test
        void shouldSkipVersionCheckWhenLocalVersionDoesNotMatchDeployedVersion() throws IOException {
            // Given
            InstanceProperties instanceProperties = createInstancePropertiesWithVersion("0.14.0-SNAPSHOT");
            SaveLocalProperties.saveToDirectory(tempDir, instanceProperties, Stream.empty());
            cdkContext.put("skipVersionCheck", "true");

            // When/Then
            assertThatCode(() -> readProps())
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldSkipVersionCheckWhenDeployingNewInstance() throws IOException {
            // Given
            InstanceProperties instanceProperties = createUserDefinedInstanceProperties();
            SaveLocalProperties.saveToDirectory(tempDir, instanceProperties, Stream.empty());

            // When/Then
            assertThatCode(() -> readProps())
                    .doesNotThrowAnyException();
        }
    }

    private InstanceProperties loadInstanceProperties() {
        return readProps().getInstanceProperties();
    }

    private SleeperInstanceProps readProps() {
        SleeperInstanceProps props = SleeperInstanceProps.fromContext(cdkContext::get, null, null);
        Stack stack = Stack.Builder.create()
                .env(Environment.builder()
                        .account(account)
                        .region(region)
                        .build())
                .build();
        SleeperNetworking networking = SleeperNetworking.createByIds(stack, vpcId, subnetIds);
        props.prepareProperties(stack, networking);
        return props;
    }

    private static InstanceProperties createInstancePropertiesWithVersion(String version) {
        InstanceProperties instanceProperties = createUserDefinedInstanceProperties();
        instanceProperties.set(VERSION, version);
        return instanceProperties;
    }

    private static InstanceProperties createUserDefinedInstanceProperties() {
        String id = UUID.randomUUID().toString().substring(0, 18);
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, id);
        instanceProperties.set(JARS_BUCKET, "test-bucket");

        return instanceProperties;
    }
}
