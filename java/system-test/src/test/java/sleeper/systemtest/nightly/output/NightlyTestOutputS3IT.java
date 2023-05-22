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
package sleeper.systemtest.nightly.output;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.io.IOUtils;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.core.CommonTestConstants;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.systemtest.nightly.output.NightlyTestOutputTestHelper.outputWithStatusCodeByTest;

@Testcontainers
class NightlyTestOutputS3IT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    private final AmazonS3 s3Client = createS3Client();
    private final String bucketName = UUID.randomUUID().toString();
    @TempDir
    private Path tempDir;

    @BeforeEach
    public void setup() {
        s3Client.createBucket(bucketName);
    }

    private AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    @Test
    void shouldUploadLogFile() throws Exception {
        // Given
        Instant startTime = Instant.parse("2023-05-04T09:35:00Z");
        Files.writeString(tempDir.resolve("bulkImportPerformance.log"), "test data");

        // When
        uploadFromTempDir(startTime);

        // Then
        assertThat(streamS3Objects())
                .contains(tuple("20230504_093500/bulkImportPerformance.log", "test data"));
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

    private void setExistingSummary(Instant startTime, Map<String, Integer> statusCodeByTest) {
        NightlyTestSummaryTable summary = NightlyTestSummaryTable.empty().add(
                NightlyTestTimestamp.from(startTime),
                outputWithStatusCodeByTest(statusCodeByTest));
        s3Client.putObject(bucketName, "summary.json", summary.toJson());
        s3Client.putObject(bucketName, "summary.txt", summary.toTableString());
    }

    private Stream<String> streamS3ObjectKeys() {
        return s3Client.listObjects(bucketName).getObjectSummaries()
                .stream().map(S3ObjectSummary::getKey);
    }

    private Stream<Tuple> streamS3Objects() {
        return streamS3ObjectKeys().map(key -> tuple(key,
                s3Client.getObjectAsString(bucketName, key)));
    }

    private void uploadFromTempDir(Instant startTime) throws Exception {
        NightlyTestOutput.from(tempDir).uploadToS3(s3Client, bucketName, NightlyTestTimestamp.from(startTime));
    }

    public static String example(String path) throws IOException {
        URL url = NightlyTestOutput.class.getClassLoader().getResource(path);
        return IOUtils.toString(Objects.requireNonNull(url), Charset.defaultCharset());
    }
}
