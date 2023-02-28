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

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

import sleeper.core.CommonTestConstants;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class SyncJarsIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    protected final S3Client s3 = S3Client.builder()
            .endpointOverride(localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                    localStackContainer.getAccessKey(), localStackContainer.getSecretKey()
            )))
            .region(Region.of(localStackContainer.getRegion()))
            .build();

    @TempDir
    private Path tempDir;
    private final String bucketName = UUID.randomUUID().toString();

    @Nested
    @DisplayName("Upload jars")
    class UploadJars {

        @Test
        void shouldCreateNewBucketIfNotPresent() throws IOException {
            // When
            syncJarsToBucket(bucketName);

            // Then
            assertThat(listObjectKeys()).isEmpty();
        }

        @Test
        void shouldUploadJars() throws IOException {
            // When
            Files.createFile(tempDir.resolve("test1.jar"));
            Files.createFile(tempDir.resolve("test2.jar"));
            syncJarsToBucket(bucketName);

            // Then
            assertThat(listObjectKeys())
                    .containsExactlyInAnyOrder("test1.jar", "test2.jar");
        }

        @Test
        void shouldIgnoreNonJarFile() throws IOException {
            // When
            Files.createFile(tempDir.resolve("test.txt"));
            syncJarsToBucket(bucketName);

            // Then
            assertThat(listObjectKeys()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Apply differences when bucket already has jars")
    class ApplyDifferences {

        @Test
        void shouldUploadNewFile() throws IOException {
            // Given
            Files.createFile(tempDir.resolve("old.jar"));
            syncJarsToBucket(bucketName);

            // When
            Files.createFile(tempDir.resolve("new.jar"));
            syncJarsToBucket(bucketName);

            // Then
            assertThat(listObjectKeys())
                    .containsExactlyInAnyOrder("old.jar", "new.jar");
        }

        @Test
        void shouldDeleteOldFile() throws IOException {
            // Given
            Files.createFile(tempDir.resolve("old.jar"));
            syncJarsToBucket(bucketName);

            // When
            Files.delete(tempDir.resolve("old.jar"));
            syncJarsToBucket(bucketName);

            // Then
            assertThat(listObjectKeys()).isEmpty();
        }

        @Test
        void shouldOnlyUploadExistingFileIfItChanged() throws IOException, InterruptedException {
            // Given
            Files.createFile(tempDir.resolve("unmodified.jar"));
            Files.writeString(tempDir.resolve("modified.jar"), "data1");
            syncJarsToBucket(bucketName);
            Instant lastModifiedBefore = getObjectLastModified("unmodified.jar");

            // When
            Thread.sleep(1000);
            Files.writeString(tempDir.resolve("modified.jar"), "data2");
            syncJarsToBucket(bucketName);

            // Then
            assertThat(getObjectLastModified("unmodified.jar"))
                    .isEqualTo(lastModifiedBefore);
            assertThat(getObjectContents("modified.jar"))
                    .isEqualTo("data2");
        }

        @Test
        void shouldNotDeleteFileIfDeleteFlagNotSet() throws IOException {
            // Given
            Files.createFile(tempDir.resolve("old.jar"));
            uploadJarsToBucket(bucketName);

            // When
            Files.delete(tempDir.resolve("old.jar"));
            uploadJarsToBucket(bucketName);

            // Then
            assertThat(listObjectKeys())
                    .containsExactly("old.jar");
        }
    }

    @Nested
    @DisplayName("Report when bucket changed")
    class ReportChanges {

        @Test
        void shouldReportChangeIfBucketCreated() throws IOException {
            // When
            boolean changed = syncJarsToBucket(bucketName);

            // Then
            assertThat(changed).isTrue();
        }

        @Test
        void shouldReportNoChangeIfBucketAlreadyExisted() throws IOException {
            // Given
            syncJarsToBucket(bucketName);

            // When
            boolean changed = syncJarsToBucket(bucketName);

            // Then
            assertThat(changed).isFalse();
        }

        @Test
        void shouldReportChangeIfFileUploaded() throws IOException {
            // Given
            syncJarsToBucket(bucketName);

            // When
            Files.createFile(tempDir.resolve("test.jar"));
            boolean changed = syncJarsToBucket(bucketName);

            // Then
            assertThat(changed).isTrue();
        }

        @Test
        void shouldReportChangeIfFileDeleted() throws IOException {
            // Given
            Files.createFile(tempDir.resolve("test.jar"));
            syncJarsToBucket(bucketName);

            // When
            Files.delete(tempDir.resolve("test.jar"));
            boolean changed = syncJarsToBucket(bucketName);

            // Then
            assertThat(changed).isTrue();
        }

        @Test
        void shouldReportNoChangeIfFileUnmodified() throws IOException {
            // Given
            Files.createFile(tempDir.resolve("test.jar"));
            syncJarsToBucket(bucketName);

            // When
            boolean changed = syncJarsToBucket(bucketName);

            // Then
            assertThat(changed).isFalse();
        }
    }

    private boolean uploadJarsToBucket(String bucketName) throws IOException {
        return syncJarsToBucket(bucketName, false);
    }

    private boolean syncJarsToBucket(String bucketName) throws IOException {
        return syncJarsToBucket(bucketName, true);
    }

    private boolean syncJarsToBucket(String bucketName, boolean deleteOld) throws IOException {
        return SyncJars.builder()
                .s3(s3)
                .jarsDirectory(tempDir)
                .region(localStackContainer.getRegion())
                .bucketName(bucketName)
                .deleteOldJars(deleteOld)
                .build().sync();
    }

    private Stream<String> listObjectKeys() {
        return s3.listObjectsV2Paginator(builder -> builder.bucket(bucketName)).stream()
                .flatMap(response -> response.contents().stream())
                .map(S3Object::key);
    }

    private Instant getObjectLastModified(String key) {
        return s3.headObject(builder -> builder.bucket(bucketName).key(key)).lastModified();
    }

    private String getObjectContents(String key) {
        return s3.getObject(builder -> builder.bucket(bucketName).key(key),
                (metadata, inputStream) -> IOUtils.toString(inputStream, Charset.defaultCharset()));
    }
}
