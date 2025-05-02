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
package sleeper.clients.deploy.jar;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;

import sleeper.clients.testutil.JarsBucketITBase;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class SyncJarsIT extends JarsBucketITBase {

    @Nested
    @DisplayName("Upload jars")
    class UploadJars {

        @Test
        void shouldCreateNewBucketIfNotPresent() throws IOException {
            // When
            uploadJarsToBucket(bucketName);

            // Then
            assertThat(listObjectKeys()).isEmpty();
        }

        @Test
        void shouldUploadJars() throws IOException {
            // When
            Files.createFile(tempDir.resolve("test1.jar"));
            Files.createFile(tempDir.resolve("test2.jar"));
            uploadJarsToBucket(bucketName);

            // Then
            assertThat(listObjectKeys())
                    .containsExactlyInAnyOrder("test1.jar", "test2.jar");
        }

        @Test
        void shouldIgnoreNonJarFile() throws IOException {
            // When
            Files.createFile(tempDir.resolve("test.txt"));
            uploadJarsToBucket(bucketName);

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
            uploadJarsToBucket(bucketName);

            // When
            Files.createFile(tempDir.resolve("new.jar"));
            uploadJarsToBucket(bucketName);

            // Then
            assertThat(listObjectKeys())
                    .containsExactlyInAnyOrder("old.jar", "new.jar");
        }

        @Test
        void shouldOnlyUploadExistingFileIfItChanged() throws IOException, InterruptedException {
            // Given
            Files.createFile(tempDir.resolve("unmodified.jar"));
            Files.writeString(tempDir.resolve("modified.jar"), "data1");
            uploadJarsToBucket(bucketName);
            Instant lastModifiedBefore = getObjectLastModified("unmodified.jar");

            // When
            Thread.sleep(1000);
            Files.writeString(tempDir.resolve("modified.jar"), "data2");
            uploadJarsToBucketDeletingOldJars(bucketName);

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
            uploadJarsToBucket(bucketName);

            // When
            Files.delete(tempDir.resolve("old.jar"));
            uploadJarsToBucketDeletingOldJars(bucketName);

            // Then
            assertThat(listObjectKeys()).isEmpty();
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
            boolean changed = uploadJarsToBucket(bucketName);

            // Then
            assertThat(changed).isTrue();
        }

        @Test
        void shouldReportNoChangeIfBucketAlreadyExisted() throws IOException {
            // Given
            uploadJarsToBucket(bucketName);

            // When
            boolean changed = uploadJarsToBucket(bucketName);

            // Then
            assertThat(changed).isFalse();
        }

        @Test
        void shouldReportChangeIfFileUploaded() throws IOException {
            // Given
            uploadJarsToBucket(bucketName);

            // When
            Files.createFile(tempDir.resolve("test.jar"));
            boolean changed = uploadJarsToBucket(bucketName);

            // Then
            assertThat(changed).isTrue();
        }

        @Test
        void shouldReportChangeIfFileDeleted() throws IOException {
            // Given
            Files.createFile(tempDir.resolve("test.jar"));
            uploadJarsToBucket(bucketName);

            // When
            Files.delete(tempDir.resolve("test.jar"));
            boolean changed = uploadJarsToBucketDeletingOldJars(bucketName);

            // Then
            assertThat(changed).isTrue();
        }

        @Test
        void shouldReportNoChangeIfFileUnmodified() throws IOException {
            // Given
            Files.createFile(tempDir.resolve("test.jar"));
            uploadJarsToBucket(bucketName);

            // When
            boolean changed = uploadJarsToBucket(bucketName);

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
            uploadJarsToBucket(bucketName);

            // Then
            assertThat(s3ClientV2.headObject(builder -> builder.bucket(bucketName).key("test.jar")))
                    .extracting(HeadObjectResponse::versionId)
                    .isNotNull();
        }

        @Test
        void shouldCreateTwoVersionsWhenUpdatingExistingJar() throws IOException, InterruptedException {
            // Given
            Files.writeString(tempDir.resolve("test.jar"), "data1");
            uploadJarsToBucket(bucketName);

            // When
            Thread.sleep(1000);
            Files.writeString(tempDir.resolve("test.jar"), "data2");
            uploadJarsToBucket(bucketName);

            // Then
            assertThat(s3ClientV2.listObjectVersionsPaginator(builder -> builder
                    .bucket(bucketName).prefix("test.jar").maxKeys(1)))
                    .flatMap(ListObjectVersionsResponse::versions)
                    .hasSize(2);
        }
    }
}
