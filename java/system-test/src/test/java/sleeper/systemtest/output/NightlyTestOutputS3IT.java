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
package sleeper.systemtest.output;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.core.CommonTestConstants;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class NightlyTestOutputS3IT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    private final AmazonS3 s3Client = createS3Client();
    private final String bucketName = UUID.randomUUID().toString();
    @TempDir
    private Path tempDir;

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
        Files.writeString(tempDir.resolve("bulkImportPerformance.log"), "test");

        // When
        uploadFromTempDir(startTime);

        // Then
        assertThat(streamS3Objects())
                .containsExactly("20230504_093500/bulkImportPerformance.log");
    }

    @Test
    @Disabled("TODO")
    void shouldUploadSummary() throws Exception {
        // Given
        Instant startTime = Instant.parse("2023-05-04T09:35:00Z");
        Files.writeString(tempDir.resolve("bulkImportPerformance.status"), "0");

        // When
        uploadFromTempDir(startTime);

        // Then
        assertThat(streamS3Objects())
                .containsExactly("summary.json", "summary.txt");
    }

    private Stream<String> streamS3Objects() {
        return s3Client.listObjects(bucketName).getObjectSummaries()
                .stream().map(S3ObjectSummary::getKey);
    }

    private void uploadFromTempDir(Instant startTime) throws Exception {
        s3Client.createBucket(bucketName);
        NightlyTestOutput.from(tempDir).uploadToS3(s3Client, bucketName, NightlyTestTimestamp.from(startTime));
    }
}
