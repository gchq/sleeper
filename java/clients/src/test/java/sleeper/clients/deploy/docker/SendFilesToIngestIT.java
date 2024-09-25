/*
 * Copyright 2022-2024 Crown Copyright
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

package sleeper.clients.deploy.docker;

import com.amazonaws.services.sqs.model.Message;
import com.google.common.io.CharStreams;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.clients.docker.SendFilesToIngest;
import sleeper.configuration.s3properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobSerDe;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;

public class SendFilesToIngestIT extends DockerInstanceTestBase {
    @TempDir
    private Path tempDir;

    @Test
    void shouldSendIngestJobForOneFile() throws Exception {
        // Given
        String instanceId = UUID.randomUUID().toString().substring(0, 18);
        deployInstance(instanceId);
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);

        Path filePath = tempDir.resolve("test-file.parquet");
        Files.writeString(filePath, "abc");

        // When
        SendFilesToIngest.uploadFilesAndSendJob(instanceProperties, "system-test", List.of(filePath), s3Client, sqsClient);

        // Then
        assertThat(getObjectContents(instanceProperties.get(DATA_BUCKET), "ingest/test-file.parquet"))
                .isEqualTo("abc");
        assertThat(sqsClient.receiveMessage(instanceProperties.get(INGEST_JOB_QUEUE_URL)).getMessages())
                .map(Message::getBody)
                .map(new IngestJobSerDe()::fromJson)
                .flatMap(IngestJob::getFiles)
                .containsExactly(instanceProperties.get(DATA_BUCKET) + "/ingest/test-file.parquet");
    }

    private String getObjectContents(String bucketName, String key) throws IOException {
        return CharStreams.toString(new InputStreamReader(s3Client.getObject(bucketName, key).getObjectContent()));
    }
}
