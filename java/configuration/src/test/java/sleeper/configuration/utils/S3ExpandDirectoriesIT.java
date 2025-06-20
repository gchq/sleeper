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
package sleeper.configuration.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.localstack.test.LocalStackTestBase;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class S3ExpandDirectoriesIT extends LocalStackTestBase {

    private S3ExpandDirectories expandDirectories = new S3ExpandDirectories(s3Client);
    private String bucket = UUID.randomUUID().toString();

    @BeforeEach
    void setUp() {
        createBucket(bucket);
    }

    @Nested
    @DisplayName("Find files by their paths")
    class FindFiles {

        @Test
        void shouldGetPathsForFilesInOneDir() throws Exception {
            // Given
            List<String> files = List.of(
                    bucket + "/file-1.parquet",
                    bucket + "/file-2.parquet");
            putObject(bucket, "file-1.parquet", "test-data");
            putObject(bucket, "file-2.parquet", "test-data");

            // When / Then
            assertThat(listPathsForJob(files))
                    .containsExactlyElementsOf(files);
        }

        @Test
        void shouldIgnoreCrcFiles() {
            // Given
            List<String> files = List.of(
                    bucket + "/file-1.parquet",
                    bucket + "/file-2.crc",
                    bucket + "/file-3.parquet");
            putObject(bucket, "file-1.parquet", "test-data");
            putObject(bucket, "file-2.crc", "test-data");
            putObject(bucket, "file-3.parquet", "test-data");

            // When / Then
            assertThat(listPathsForJob(files))
                    .containsExactlyInAnyOrder(
                            bucket + "/file-1.parquet",
                            bucket + "/file-3.parquet");
        }

        @Test
        void shouldReadFileSizes() throws Exception {
            // Given
            List<String> files = List.of(
                    bucket + "/file-1.parquet",
                    bucket + "/file-2.parquet");
            putObject(bucket, "file-1.parquet", "this is a short test file contents");
            putObject(bucket, "file-2.parquet", "this is a longer test file contents with " +
                    "more details for a bigger number of size");

            // When / Then
            assertThat(listFileSizeBytes(files))
                    .containsExactlyInAnyOrder(34L, 81L);
        }
    }

    @Nested
    @DisplayName("Find files by paths of directories")
    class FollowDirectories {

        @Test
        void shouldGetPathsForFilesInADirectory() {
            // Given
            List<String> files = List.of(bucket + "/test-folder");
            String objectKey1 = "test-folder/file-1.parquet";
            String objectKey2 = "test-folder/file-2.parquet";
            putObject(bucket, objectKey1, "test-data");
            putObject(bucket, objectKey2, "test-data");

            // When / Then
            assertThat(listPathsForJob(files))
                    .containsExactlyInAnyOrder(
                            bucket + "/" + objectKey1,
                            bucket + "/" + objectKey2);
        }

        @Test
        void shouldGetPathsForFilesInNestedDirectories() {
            // Given
            List<String> files = List.of(
                    bucket + "/folder-1",
                    bucket + "/folder-2");
            String objectKey1 = "folder-1/file-1.parquet";
            String objectKey2 = "folder-1/subfolder/file-2.parquet";
            String objectKey3 = "folder-2/file-3.parquet";
            String objectKey4 = "folder-2/subfolder/otherfolder/file-4.parquet";
            putObject(bucket, objectKey1, "test-data");
            putObject(bucket, objectKey2, "test-data");
            putObject(bucket, objectKey3, "test-data");
            putObject(bucket, objectKey4, "test-data");

            // When / Then
            assertThat(listPathsForJob(files))
                    .containsExactlyInAnyOrder(
                            bucket + "/" + objectKey1,
                            bucket + "/" + objectKey2,
                            bucket + "/" + objectKey3,
                            bucket + "/" + objectKey4);
        }

        @Test
        void shouldReadFileSizesUnderADirectory() throws Exception {
            // Given
            List<String> files = List.of(bucket + "/size-test-folder");
            String objectKey1 = "size-test-folder/file-1.parquet";
            String objectKey2 = "size-test-folder/subfolder/file-2.parquet";

            putObject(bucket, objectKey1, "this is a sub folder size test");
            putObject(bucket, objectKey2, "this is a longer test file for sub folder evaluation it has " +
                    "more details for a bigger number of size");

            // When / Then
            assertThat(listFileSizeBytes(files))
                    .containsExactlyInAnyOrder(30L, 100L);
        }
    }

    @Nested
    @DisplayName("Find no files when none are present")
    class FindNoFiles {
        @Test
        void shouldReturnEmptyStreamIfNoFiles() throws Exception {
            assertThat(listPathsForJob(List.of())).isEmpty();
        }

        @Test
        void shouldReturnEmptyStreamIfNull() throws Exception {
            assertThat(listPathsForJob(null)).isEmpty();
        }

        @Test
        void shouldReturnEmptyContentsWhenFileIsNotFound() {
            // Given
            String file = bucket + "/not-a-file.parquet";

            // When / Then
            assertThat(expandDirectories.expandPaths(List.of(file)).listMissingPaths())
                    .containsExactly(file);
        }

        @Test
        void shouldReturnEmptyContentsWhenDirectoryIsNotFound() {
            // Given
            String file = bucket + "/not-a-directory/";

            // When / Then
            assertThat(expandDirectories.expandPaths(List.of(file)).listMissingPaths())
                    .containsExactly(file);
        }
    }

    private List<Long> listFileSizeBytes(List<String> files) {
        return expandDirectories.expandPaths(files)
                .streamFilesThrowIfAnyPathIsEmpty()
                .map(S3FileDetails::fileSizeBytes)
                .toList();
    }

    private List<String> listPathsForJob(List<String> files) {
        return expandDirectories.expandPaths(files).listJobPaths();
    }

}
