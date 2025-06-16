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
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import sleeper.configuration.utils.S3PathUtils.S3FileDetails;
import sleeper.localstack.test.LocalStackTestBase;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3PathUtilsIT extends LocalStackTestBase {

    private S3PathUtils s3PathUtils = new S3PathUtils(s3Client);
    private String bucket = UUID.randomUUID().toString();

    @BeforeEach
    void setUp() {
        createBucket(bucket);
    }

    @Test
    void shouldGetPathsForFilesInOneDir() throws Exception {
        // Given
        String folder = "test-folder";

        createTestFile(folder + "/file-1.parquet");
        createTestFile(folder + "/file-2.parquet");

        // When
        List<S3FileDetails> paths = s3PathUtils.streamFileDetails(bucket, folder);

        // Then
        assertThat(paths)
                .extracting(path -> path.filename())
                .containsExactlyInAnyOrder(generateFilePath(folder, "/file-1.parquet"),
                        generateFilePath(folder, "/file-2.parquet"));
    }

    @Test
    void shouldReadFileSizes() throws Exception {
        // Given
        String folder = "size-test-folder";

        createTestFileWithContents(folder + "/file-1.parquet", "this is a short test file contents");
        createTestFileWithContents(folder + "/file-2.parquet", "this is a longer test file contents with " +
                "more details for a bigger number of size");

        // When
        List<S3FileDetails> fileDetails = s3PathUtils.streamFileDetails(bucket, folder);

        // Then
        assertThat(fileDetails)
                .extracting(fileSize -> fileSize.fileSizeBytes())
                .containsExactlyInAnyOrder(34L, 81L);
    }

    @Test
    void shouldReturnEmptyListIfNoFiles() throws Exception {
        // Given / When
        List<S3FileDetails> emptyDetails = s3PathUtils.streamFileDetails(bucket, "");

        // Then
        assertThat(emptyDetails).isEmpty();
    }

    @Test
    void shouldExceptionBucketIncorrect() {
        // When / Then
        assertThatThrownBy(() -> s3PathUtils.streamFileDetails(UUID.randomUUID().toString(), "test"))
                .isInstanceOf(NoSuchBucketException.class);
    }

    private void createTestFileWithContents(String filename, String contents) {
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(bucket)
                .key(filename)
                .build(),
                RequestBody.fromString(contents));
    }

    private void createTestFile(String filename) {
        createTestFileWithContents(filename, "dummy-test-data");
    }

    private String generateFilePath(String folder, String filename) {
        return bucket + "/" + folder + filename;
    }

}
