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

package sleeper.ingest.batcher.submitter;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.core.CommonTestConstants;
import sleeper.ingest.batcher.FileIngestRequest;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.batcher.submitter.IngestBatcherSubmitterLambda.isRequestForDirectory;

@Testcontainers
public class IngestBatcherSubmitterLambdaIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    protected final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
            .withCredentials(localStackContainer.getDefaultCredentialsProvider())
            .build();

    private static final String TEST_BUCKET = "test-bucket";

    @BeforeEach
    void setup() {
        s3.createBucket(TEST_BUCKET);

    }

    @AfterEach
    void tearDown() {
        s3.listObjects(TEST_BUCKET).getObjectSummaries().forEach(s3ObjectSummary ->
                s3.deleteObject(TEST_BUCKET, s3ObjectSummary.getKey()));
        s3.deleteBucket(TEST_BUCKET);
    }

    @Test
    void shouldDetectThatRequestIsForDirectory() {
        // Given
        s3.putObject(TEST_BUCKET, "test-directory/test-1.parquet", "test");
        FileIngestRequest request = FileIngestRequest.builder()
                .file(TEST_BUCKET + "/test-directory")
                .fileSizeBytes(123)
                .tableName("test-table")
                .receivedTime(Instant.parse("2023-06-15T15:30:00Z")).build();

        // When/Then
        assertThat(isRequestForDirectory(s3, request)).isTrue();
    }
}
