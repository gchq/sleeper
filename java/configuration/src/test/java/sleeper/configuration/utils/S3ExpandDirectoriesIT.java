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

import sleeper.configuration.utils.S3ExpandDirectories.S3FileDetails;
import sleeper.localstack.test.LocalStackTestBase;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
            String folder = "test-folder";
            putObject(bucket, folder + "/file-1.parquet", "test-data");
            putObject(bucket, folder + "/file-2.parquet", "test-data");

            List<String> files = List.of(appendBucketNamePrefix(folder));

            // When
            Stream<String> paths = expandDirectories.streamFilenames(files);

            // Then
            assertThat(paths)
                    .containsExactlyInAnyOrder(generateFilePath(folder, "/file-1.parquet"),
                            generateFilePath(folder, "/file-2.parquet"));
        }

        @Test
        void shouldIgnoreCrcFiles() {
            // Given
            String folder = "test-folder";
            putObject(bucket, folder + "/file-1.parquet", "test-data");
            putObject(bucket, folder + "/file-2.crc", "test-data");
            putObject(bucket, folder + "/file-3.parquet", "test-data");

            List<String> files = List.of(appendBucketNamePrefix(folder));

            // When
            Stream<String> paths = expandDirectories.streamFilenames(files);

            // Then
            assertThat(paths)
                    .containsExactlyInAnyOrder(generateFilePath(folder, "/file-1.parquet"),
                            generateFilePath(folder, "/file-3.parquet"));
        }

        @Test
        void shouldReadFileSizes() throws Exception {
            // Given
            String folder = "size-test-folder";

            putObject(bucket, folder + "/file-1.parquet", "this is a short test file contents");
            putObject(bucket, folder + "/file-2.parquet", "this is a longer test file contents with " +
                    "more details for a bigger number of size");
            List<String> files = List.of(appendBucketNamePrefix(folder));

            // When
            Stream<S3FileDetails> fileDetails = expandDirectories.streamFilesAsS3FileDetails(files);

            // Then
            assertThat(fileDetails)
                    .extracting(file -> file.fileObject().size())
                    .containsExactlyInAnyOrder(34L, 81L);
        }
    }

    @Nested
    @DisplayName("Find files by paths of directories")
    class FollowDirectories {

        @Test
        void shouldGetPathsForFilesInMultipleDirectories() {
            // Given
            String folder1 = "test-folder-1";
            String folder2 = "test-folder-2";
            putObject(bucket, folder1 + "/file-1.parquet", "test-data");
            putObject(bucket, folder2 + "/file-2.parquet", "test-data");
            List<String> files = List.of(appendBucketNamePrefix(folder1),
                    appendBucketNamePrefix(folder2));

            // When
            Stream<String> paths = expandDirectories.streamFilenames(files);

            // Then
            assertThat(paths)
                    .containsExactlyInAnyOrder(generateFilePath(folder1, "/file-1.parquet"),
                            generateFilePath(folder2, "/file-2.parquet"));
        }

        @Test
        void shouldGetPathsForFilesInNestedDirectories() {
            // Given
            String folder1 = "test-folder-1";
            String subfolder1 = "/sub-folder-1";
            String folder2 = "test-folder-2";
            putObject(bucket, folder1 + subfolder1 + "/file-1.parquet", "test-data");
            putObject(bucket, folder2 + "/file-2.parquet", "test-data");
            List<String> files = List.of(appendBucketNamePrefix(folder1),
                    appendBucketNamePrefix(folder2));

            // When
            Stream<String> paths = expandDirectories.streamFilenames(files);

            // Then
            assertThat(paths)
                    .containsExactlyInAnyOrder(generateFilePath(folder1, (subfolder1 + "/file-1.parquet")),
                            generateFilePath(folder2, "/file-2.parquet"));
        }

        @Test
        void shouldReadFileSizesUnderADirectory() throws Exception {
            // Given
            String folder = "size-test-folder";
            String subfolder = "/subfolder";

            putObject(bucket, folder + subfolder + "/file-1.parquet", "this is a sub folder size test");
            putObject(bucket, folder + subfolder + "/file-2.parquet", "this is a longer test file for sub folder evaluation it has " +
                    "more details for a bigger number of size");
            List<String> files = List.of(appendBucketNamePrefix(folder));

            // When
            Stream<S3FileDetails> fileDetails = expandDirectories.streamFilesAsS3FileDetails(files);

            // Then
            assertThat(fileDetails)
                    .extracting(file -> file.fileObject().size())
                    .containsExactlyInAnyOrder(30L, 100L);
        }

        @Test
        void shouldFailWhenFileNotFoundAtSpecifiedPath() {
            assertThatThrownBy(() -> expandDirectories.listFilesAsS3FileDetails(bucket + "/not-a-file.parquet"))
                    .isInstanceOf(S3FileNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("Find no files when none are present")
    class FindNoFiles {
        @Test
        void shouldReturnEmptyStreamIfNoFiles() throws Exception {
            assertThat(expandDirectories.streamFilesAsS3FileDetails(List.of())).isEmpty();
        }

        @Test
        void shouldReturnEmptyStreamIfNull() throws Exception {
            assertThat(expandDirectories.streamFilesAsS3FileDetails(null)).isEmpty();
        }

        @Test
        void shouldFailWhenFileNotFoundAtSpecifiedPath() {
            assertThatThrownBy(() -> expandDirectories.streamFilesAsS3FileDetails(List.of(bucket + "/not-a-file.parquet")).toList())
                    .isInstanceOf(S3FileNotFoundException.class);
        }
    }

    private String appendBucketNamePrefix(String file) {
        return bucket + "/" + file;
    }

    private String generateFilePath(String folder, String filename) {
        return appendBucketNamePrefix(folder + filename);
    }

}
