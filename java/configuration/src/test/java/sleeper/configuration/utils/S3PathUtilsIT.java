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
import software.amazon.awssdk.services.s3.model.S3Object;

import sleeper.localstack.test.LocalStackTestBase;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

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
            List<String> files = List.of(createTestFile(folder + "/file-1.parquet"),
                    createTestFile(folder + "/file-2.parquet"));

            // When
            Stream<String> paths = s3PathUtils.streamFilenames(files);

            // Then
            assertThat(paths)
                    .containsExactlyInAnyOrder(generateFilePath(folder, "/file-1.parquet"),
                            generateFilePath(folder, "/file-2.parquet"));
        }

        @Test
        void shouldReadFileSizes() throws Exception {
            // Given
            String folder = "size-test-folder";
            List<String> files = List.of(
                    createTestFileWithContents(folder + "/file-1.parquet", "this is a short test file contents"),
                    createTestFileWithContents(folder + "/file-2.parquet", "this is a longer test file contents with " +
                            "more details for a bigger number of size"));

            // When
            Stream<S3Object> fileDetails = s3PathUtils.streamFilesAsS3Objects(files);

            // Then
            assertThat(fileDetails)
                    .extracting(fileSize -> fileSize.size())
                    .containsExactlyInAnyOrder(34L, 81L);
        }
    }

    @Nested
    @DisplayName("Find no files when none are present")
    class FindNoFiles {
        @Test
        void shouldReturnEmptyListIfNoFiles() throws Exception {
            assertThat(s3PathUtils.streamFilenames(List.of())).isEmpty();
        }
    }

    private String createTestFileWithContents(String filename, String contents) {
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(bucket)
                .key(filename)
                .build(),
                RequestBody.fromString(contents));
        return bucket + "/" + filename;
    }

    private String createTestFile(String filename) {
        return createTestFileWithContents(filename, "dummy-test-data");
    }

    private String generateFilePath(String folder, String filename) {
        return folder + filename;
    }

}
