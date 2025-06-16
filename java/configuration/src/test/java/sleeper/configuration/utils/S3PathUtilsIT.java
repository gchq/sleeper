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
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import sleeper.configuration.utils.S3PathUtils.S3FileDetails;
import sleeper.localstack.test.LocalStackTestBase;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3PathUtilsIT extends LocalStackTestBase {

    private S3PathUtils s3PathUtils = new S3PathUtils(s3Client);
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
            createTestFile(folder + "/file-1.parquet");
            createTestFile(folder + "/file-2.parquet");

            List<String> files = List.of(appendBucketNamePrefix(folder));

            // When
            Stream<String> paths = s3PathUtils.streamFilenames(files);

            // Then
            assertThat(paths)
                    .containsExactlyInAnyOrder(generateFilePath(folder, "/file-1.parquet"),
                            generateFilePath(folder, "/file-2.parquet"));
        }

        @Test
        void shouldIgnoreCrcFiles() {
            // Given
            String folder = "test-folder";
            createTestFile(folder + "/file-1.parquet");
            createTestFile(folder + "/file-2.crc");
            createTestFile(folder + "/file-3.parquet");

            List<String> files = List.of(appendBucketNamePrefix(folder));

            // When
            Stream<String> paths = s3PathUtils.streamFilenames(files);

            // Then
            assertThat(paths)
                    .containsExactlyInAnyOrder(generateFilePath(folder, "/file-1.parquet"),
                            generateFilePath(folder, "/file-3.parquet"));
        }

        @Test
        void shouldReadFileSizes() throws Exception {
            // Given
            String folder = "size-test-folder";

            createTestFileWithContents(folder + "/file-1.parquet", "this is a short test file contents");
            createTestFileWithContents(folder + "/file-2.parquet", "this is a longer test file contents with " +
                    "more details for a bigger number of size");
            List<String> files = List.of(appendBucketNamePrefix(folder));

            // When
            Stream<S3FileDetails> fileDetails = s3PathUtils.streamFilesAsS3FileDetails(files);

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
            createTestFile(folder1 + "/file-1.parquet");
            createTestFile(folder2 + "/file-2.parquet");
            List<String> files = List.of(appendBucketNamePrefix(folder1),
                    appendBucketNamePrefix(folder2));

            // When
            Stream<String> paths = s3PathUtils.streamFilenames(files);

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
            createTestFile(folder1 + subfolder1 + "/file-1.parquet");
            createTestFile(folder2 + "/file-2.parquet");
            List<String> files = List.of(appendBucketNamePrefix(folder1),
                    appendBucketNamePrefix(folder2));

            // When
            Stream<String> paths = s3PathUtils.streamFilenames(files);

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

            createTestFileWithContents(folder + subfolder + "/file-1.parquet", "this is a sub folder size test");
            createTestFileWithContents(folder + subfolder + "/file-2.parquet", "this is a longer test file for sub folder evaluation it has " +
                    "more details for a bigger number of size");
            List<String> files = List.of(appendBucketNamePrefix(folder));

            // When
            Stream<S3FileDetails> fileDetails = s3PathUtils.streamFilesAsS3FileDetails(files);

            // Then
            assertThat(fileDetails)
                    .extracting(file -> file.fileObject().size())
                    .containsExactlyInAnyOrder(30L, 100L);
        }

        @Test
        void shouldFailWhenFileNotFoundAtSpecifiedPath() {
            assertThatThrownBy(() -> s3PathUtils.listFilesAsS3FileDetails(bucket + "/not-a-file.parquet"))
                    .isInstanceOf(FileNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("Find no files when none are present")
    class FindNoFiles {
        @Test
        void shouldReturnEmptyListIfNoFiles() throws Exception {
            assertThat(s3PathUtils.streamFilenames(List.of())).isEmpty();
        }

        @Test
        void shouldReturnEmptyListForFilesNotFound() {
            // Given
            List<String> files = List.of((bucket + "/not-file-1.parquet"),
                    (bucket + "/not-file-2.parquet"));

            // When /Then
            assertThat(s3PathUtils.streamFilenames(files)).isEmpty();
        }
    }

    private void createTestFileWithContents(String filename, String contents) {
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(bucket)
                .key(filename)
                .build(),
                RequestBody.fromString(contents));
    }

    private String appendBucketNamePrefix(String file) {
        return bucket + "/" + file;
    }

    private void createTestFile(String filename) {
        createTestFileWithContents(filename, "dummy-test-data");
    }

    private String generateFilePath(String folder, String filename) {
        return appendBucketNamePrefix(folder + filename);
    }

}
