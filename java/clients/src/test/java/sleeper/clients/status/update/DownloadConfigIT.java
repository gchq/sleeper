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
package sleeper.clients.status.update;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.CommonTestConstants;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.deploy.GenerateInstanceProperties.generateDefaultsFromInstanceId;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;

@Testcontainers
public class DownloadConfigIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.S3);

    private final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
            .withCredentials(localStackContainer.getDefaultCredentialsProvider())
            .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
            .build();

    @TempDir
    private Path testDir;
    private Path targetDir;
    private Path tempDir;

    @BeforeEach
    void setUp() {
        targetDir = testDir.resolve("generated");
        tempDir = testDir.resolve("temp");
    }

    @Test
    void shouldCreateDirectoryAndDownloadInstanceProperties() throws IOException {
        // Given
        InstanceProperties properties = createTestInstanceProperties(s3);

        // When
        InstanceProperties found = DownloadConfig.overwriteTargetDirectoryIfDownloadSuccessful(s3, properties.get(ID), targetDir, tempDir);

        // Then
        assertThat(found).isEqualTo(properties);
        assertThat(Files.walk(targetDir)).containsExactlyInAnyOrder(
                targetDir,
                targetDir.resolve("instance.properties"),
                targetDir.resolve("tags.properties"),
                targetDir.resolve("configBucket.txt"));
        assertThat(tempDir).isEmptyDirectory();
    }

    @Test
    void shouldClearDirectoryAndDownloadInstanceProperties() throws IOException {
        // Given
        InstanceProperties properties = createTestInstanceProperties(s3);
        Files.createDirectories(targetDir);
        Files.writeString(targetDir.resolve("test1.txt"), "data");
        Files.createDirectories(tempDir);
        Files.writeString(tempDir.resolve("test2.txt"), "data");

        // When
        InstanceProperties found = DownloadConfig.overwriteTargetDirectoryIfDownloadSuccessful(s3, properties.get(ID), targetDir, tempDir);

        // Then
        assertThat(found).isEqualTo(properties);
        assertThat(Files.walk(targetDir)).containsExactlyInAnyOrder(
                targetDir,
                targetDir.resolve("instance.properties"),
                targetDir.resolve("tags.properties"),
                targetDir.resolve("configBucket.txt"));
        assertThat(tempDir).isEmptyDirectory();
    }

    @Test
    void shouldGenerateDefaultsIfMissing() throws IOException {

        // When
        InstanceProperties found = DownloadConfig.overwriteTargetDirectoryGenerateDefaultsIfMissing(s3, "test-instance", targetDir, tempDir);

        // Then
        assertThat(found).isEqualTo(generateDefaultsFromInstanceId("test-instance"));
        assertThat(Files.walk(targetDir)).containsExactlyInAnyOrder(
                targetDir,
                targetDir.resolve("instance.properties"),
                targetDir.resolve("tags.properties"),
                targetDir.resolve("configBucket.txt"),
                targetDir.resolve("queryResultsBucket.txt"));
        assertThat(tempDir).isEmptyDirectory();
    }

    @Test
    void shouldClearDirectoryAndDownloadInstancePropertiesInsteadOfGeneratingDefaultsIfPresent() throws IOException {
        // Given
        InstanceProperties properties = createTestInstanceProperties(s3);
        Files.createDirectories(targetDir);
        Files.writeString(targetDir.resolve("test1.txt"), "data");
        Files.createDirectories(tempDir);
        Files.writeString(tempDir.resolve("test2.txt"), "data");

        // When
        InstanceProperties found = DownloadConfig.overwriteTargetDirectoryGenerateDefaultsIfMissing(s3, properties.get(ID), targetDir, tempDir);

        // Then
        assertThat(found).isEqualTo(properties);
        assertThat(Files.walk(targetDir)).containsExactlyInAnyOrder(
                targetDir,
                targetDir.resolve("instance.properties"),
                targetDir.resolve("tags.properties"),
                targetDir.resolve("configBucket.txt"));
        assertThat(tempDir).isEmptyDirectory();
    }
}
