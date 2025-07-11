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
package sleeper.systemtest.drivers.nightly;

import com.google.common.io.CharStreams;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.S3Object;

import sleeper.localstack.test.LocalStackTestBase;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.systemtest.drivers.nightly.NightlyTestOutputTestHelper.outputWithStatusCodeByTest;

class NightlyTestOutputS3IT extends LocalStackTestBase {

    private final String bucketName = UUID.randomUUID().toString();
    @TempDir
    private Path tempDir;

    @BeforeEach
    public void setup() {
        createBucket(bucketName);
    }

    @Test
    void shouldUploadLogFiles() throws Exception {
        // Given
        Instant startTime = Instant.parse("2023-05-04T09:35:00Z");
        Files.writeString(tempDir.resolve("maven.log"), "root log test");
        Files.createDirectory(tempDir.resolve("maven"));
        Files.writeString(tempDir.resolve("maven/IngestBatcherIT.shouldCreateTwoJobs.report.log"), "nested log test");

        // When
        uploadFromTempDir(startTime);

        // Then
        assertThat(streamS3Objects())
                .contains(
                        tuple("20230504_093500/maven.log", "root log test"),
                        tuple("20230504_093500/maven/IngestBatcherIT.shouldCreateTwoJobs.report.log", "nested log test"));
    }

    @Test
    void shouldUploadSummary() throws Exception {
        // Given
        Instant startTime = Instant.parse("2023-05-04T09:35:00Z");
        Files.writeString(tempDir.resolve("bulkImportPerformance.status"), "0");

        // When
        uploadFromTempDir(startTime);

        // Then
        assertThat(streamS3Objects())
                .containsExactly(
                        tuple("summary.json", example("nightlyTest/uploadSummary.json")),
                        tuple("summary.txt", example("nightlyTest/uploadSummary.txt")));
    }

    @Test
    void shouldUpdateSummaryIfAlreadyExists() throws Exception {
        // Given
        setExistingSummary(Instant.parse("2023-05-04T09:35:00Z"), Map.of("bulkImportPerformance", 0));
        Instant startTime = Instant.parse("2023-05-04T09:40:00Z");
        Files.writeString(tempDir.resolve("bulkImportPerformance.status"), "0");

        // When
        uploadFromTempDir(startTime);

        // Then
        assertThat(streamS3Objects())
                .containsExactly(
                        tuple("summary.json", example("nightlyTest/updateExistingSummary.json")),
                        tuple("summary.txt", example("nightlyTest/updateExistingSummary.txt")));
    }

    @Test
    void shouldUploadSiteFile() throws Exception {
        // Given
        Instant startTime = Instant.parse("2023-05-04T09:45:00Z");
        Files.writeString(tempDir.resolve("test-site.zip"), "test data");

        // When
        uploadFromTempDir(startTime);

        // Then
        assertThat(streamS3Objects())
                .contains(
                        tuple("20230504_094500/test-site.zip", "test data"));
    }

    private void setExistingSummary(Instant startTime, Map<String, Integer> statusCodeByTest) {
        NightlyTestSummaryTable summary = NightlyTestSummaryTable.empty().add(
                NightlyTestTimestamp.from(startTime),
                outputWithStatusCodeByTest(statusCodeByTest));
        s3Client.putObject(
                request -> request.bucket(bucketName).key("summary.json"),
                RequestBody.fromString(summary.toJson()));
        s3Client.putObject(
                request -> request.bucket(bucketName).key("summary.txt"),
                RequestBody.fromString(summary.toTableString()));
    }

    private Stream<String> streamS3ObjectKeys() {
        return s3Client.listObjectsV2(request -> request.bucket(bucketName))
                .contents().stream().map(S3Object::key);
    }

    private Stream<Tuple> streamS3Objects() {
        return streamS3ObjectKeys().map(key -> tuple(key,
                s3Client.getObject(
                        request -> request.bucket(bucketName).key(key),
                        ResponseTransformer.toBytes())
                        .asUtf8String()));
    }

    private void uploadFromTempDir(Instant startTime) throws Exception {
        NightlyTestOutput.from(tempDir).uploadToS3(s3Client, bucketName, NightlyTestTimestamp.from(startTime));
    }

    public static String example(String path) throws IOException {
        try (Reader reader = new InputStreamReader(NightlyTestOutputS3IT.class.getClassLoader().getResourceAsStream(path))) {
            return CharStreams.toString(reader);
        }
    }
}
