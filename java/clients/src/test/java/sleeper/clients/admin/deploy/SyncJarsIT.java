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
package sleeper.clients.admin.deploy;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.core.CommonTestConstants;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class SyncJarsIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    protected final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
            .withCredentials(localStackContainer.getDefaultCredentialsProvider())
            .build();

    @TempDir
    private Path tempDir;
    private final String bucketName = UUID.randomUUID().toString();

    @Test
    void shouldCreateNewBucketIfNotPresent() throws IOException {
        // When
        syncJarsToBucket(bucketName);

        // Then
        assertThat(s3.listObjectsV2(bucketName).getObjectSummaries()).isEmpty();
    }

    @Test
    void shouldUploadJars() throws IOException {
        // When
        Files.createFile(tempDir.resolve("test1.jar"));
        Files.createFile(tempDir.resolve("test2.jar"));
        syncJarsToBucket(bucketName);

        // Then
        assertThat(s3.listObjectsV2(bucketName).getObjectSummaries())
                .extracting(S3ObjectSummary::getKey)
                .containsExactlyInAnyOrder("test1.jar", "test2.jar");
    }

    @Test
    void shouldApplyChangesToJars() throws IOException, InterruptedException {
        // Given
        Files.createFile(tempDir.resolve("unmodified.jar"));
        Files.writeString(tempDir.resolve("modified.jar"), "data1");
        Files.createFile(tempDir.resolve("deleted.jar"));
        syncJarsToBucket(bucketName);
        Date lastModifiedBefore = s3.getObjectMetadata(bucketName, "unmodified.jar").getLastModified();

        // When
        Thread.sleep(1000);
        Files.delete(tempDir.resolve("deleted.jar"));
        Files.createFile(tempDir.resolve("added.jar"));
        Files.writeString(tempDir.resolve("modified.jar"), "data2");
        syncJarsToBucket(bucketName);

        // Then
        assertThat(S3Objects.inBucket(s3, bucketName))
                .extracting(S3ObjectSummary::getKey)
                .containsExactlyInAnyOrder("added.jar", "modified.jar", "unmodified.jar");
        assertThat(s3.getObjectMetadata(bucketName, "unmodified.jar"))
                .extracting(ObjectMetadata::getLastModified)
                .isEqualTo(lastModifiedBefore);
        assertThat(s3.getObjectAsString(bucketName, "modified.jar"))
                .isEqualTo("data2");
    }

    private void syncJarsToBucket(String bucketName) throws IOException {
        SyncJars.builder()
                .s3(s3).jarsDirectory(tempDir)
                .bucketName(bucketName)
                .build().sync();
    }
}
