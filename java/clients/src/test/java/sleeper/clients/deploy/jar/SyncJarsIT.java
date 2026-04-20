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
package sleeper.clients.deploy.jar;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;

import sleeper.clients.testutil.JarsBucketITBase;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;

class SyncJarsIT extends JarsBucketITBase {

    @Nested
    @DisplayName("Upload jars")
    class UploadJars {

        @Test
        void shouldUploadJars() throws IOException {
            // When
            Files.createFile(tempDir.resolve("test1.jar"));
            Files.createFile(tempDir.resolve("test2.jar"));
            uploadJarsToBucket();

            // Then
            assertThat(listObjectKeys()).isEqualTo(Set.of("test1.jar", "test2.jar"));
        }

        @Test
        void shouldIgnoreNonJarFile() throws IOException {
            // When
            Files.createFile(tempDir.resolve("test.txt"));
            uploadJarsToBucket();

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
            uploadJarsToBucket();

            // When
            Files.createFile(tempDir.resolve("new.jar"));
            uploadJarsToBucket();

            // Then
            assertThat(listObjectKeys()).isEqualTo(Set.of("old.jar", "new.jar"));
        }

        @Test
        void shouldOnlyUploadExistingFileIfItChanged() throws IOException, InterruptedException {
            // Given
            Files.createFile(tempDir.resolve("unmodified.jar"));
            Files.writeString(tempDir.resolve("modified.jar"), "data1");
            uploadJarsToBucket();
            Instant lastModifiedBefore = getObjectLastModified("unmodified.jar");

            // When
            Thread.sleep(1000);
            Files.writeString(tempDir.resolve("modified.jar"), "data2");
            uploadJarsToBucketDeletingOldJars();

            // Then
            assertThat(getObjectLastModified("unmodified.jar"))
                    .isEqualTo(lastModifiedBefore);
            assertThat(getObjectContents("modified.jar"))
                    .isEqualTo("data2");
        }

        @Test
        void shouldDeleteOldFileWhenDeleteFlagIsSet() throws IOException {
            // Given
            Files.createFile(tempDir.resolve("old.jar"));
            uploadJarsToBucket();

            // When
            Files.delete(tempDir.resolve("old.jar"));
            uploadJarsToBucketDeletingOldJars();

            // Then
            assertThat(listObjectKeys()).isEmpty();
        }

        @Test
        void shouldNotDeleteFileIfDeleteFlagNotSet() throws IOException {
            // Given
            Files.createFile(tempDir.resolve("old.jar"));
            uploadJarsToBucket();

            // When
            Files.delete(tempDir.resolve("old.jar"));
            uploadJarsToBucket();

            // Then
            assertThat(listObjectKeys()).isEqualTo(Set.of("old.jar"));
        }
    }

    @Nested
    @DisplayName("Report when bucket changed")
    class ReportChanges {

        @Test
        void shouldReportNoChangeIfBucketAlreadyExisted() throws IOException {
            // Given
            uploadJarsToBucket();

            // When
            boolean changed = uploadJarsToBucket();

            // Then
            assertThat(changed).isFalse();
        }

        @Test
        void shouldReportChangeIfFileUploaded() throws IOException {
            // Given
            uploadJarsToBucket();

            // When
            Files.createFile(tempDir.resolve("test.jar"));
            boolean changed = uploadJarsToBucket();

            // Then
            assertThat(changed).isTrue();
        }

        @Test
        void shouldReportChangeIfFileDeleted() throws IOException {
            // Given
            Files.createFile(tempDir.resolve("test.jar"));
            uploadJarsToBucket();

            // When
            Files.delete(tempDir.resolve("test.jar"));
            boolean changed = uploadJarsToBucketDeletingOldJars();

            // Then
            assertThat(changed).isTrue();
        }

        @Test
        void shouldReportNoChangeIfFileUnmodified() throws IOException {
            // Given
            Files.createFile(tempDir.resolve("test.jar"));
            uploadJarsToBucket();

            // When
            boolean changed = uploadJarsToBucket();

            // Then
            assertThat(changed).isFalse();
        }
    }

    @Nested
    @DisplayName("Save version when jars uploaded")
    class SaveVersions {
        @Test
        void shouldCreateVersionsForNewJars() throws IOException {
            // Given
            Files.createFile(tempDir.resolve("test.jar"));

            // When
            uploadJarsToBucket();

            // Then
            assertThat(s3Client.headObject(builder -> builder.bucket(bucketName).key("test.jar")))
                    .extracting(HeadObjectResponse::versionId)
                    .isNotNull();
        }

        @Test
        void shouldCreateTwoVersionsWhenUpdatingExistingJar() throws IOException, InterruptedException {
            // Given
            Files.writeString(tempDir.resolve("test.jar"), "data1");
            uploadJarsToBucket();

            // When
            Thread.sleep(1000);
            Files.writeString(tempDir.resolve("test.jar"), "data2");
            uploadJarsToBucket();

            // Then
            assertThat(s3Client.listObjectVersionsPaginator(builder -> builder
                    .bucket(bucketName).prefix("test.jar").maxKeys(1)))
                    .flatMap(ListObjectVersionsResponse::versions)
                    .hasSize(2);
        }
    }

    @Nested
    @DisplayName("Override default bucket name")
    class OverrideBucketName {

        @BeforeEach
        void setUp() throws Exception {
            Files.writeString(tempDir.resolve("test.jar"), "data");
        }

        @Test
        void shouldUseDefaultBucketName() throws Exception {
            // Given
            instanceProperties.unset(JARS_BUCKET);

            // When
            uploadJarsToBucket();

            // Then
            assertThat(listObjectKeys()).containsExactly("test.jar");
        }

        @Test
        void shouldOverrideBucketName() throws Exception {
            // Given
            String override = UUID.randomUUID().toString();
            createBucket(override);
            instanceProperties.set(JARS_BUCKET, override);

            // When
            uploadJarsToBucket();

            // Then
            assertThat(listObjectKeys()).isEmpty();
            assertThat(listObjectKeys(override)).containsExactly("test.jar");
        }
    }
}
