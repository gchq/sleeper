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

package sleeper.clients.deploy.docker;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.clients.docker.DeployDockerInstance;
import sleeper.clients.docker.WriteRandomData;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.testutils.LocalStackAwsV2ClientHelper.buildAwsV2Client;

public class WriteRandomDataIT extends DockerInstanceTestBase {
    @Test
    void shouldWriteRandomDataToInstance() throws Exception {
        // Given
        DeployDockerInstance.deploy("write-random-data", s3Client, dynamoDB, sqsClient);
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3Client, "write-random-data");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.loadFromS3(s3Client, "system-test");

        // When
        WriteRandomData.writeWithIngestFactory(
                instanceProperties, tableProperties, dynamoDB,
                getHadoopConfiguration(), createS3AsyncClient(), 1000);

        // Then
        assertThat(queryAllRecords(instanceProperties, tableProperties))
                .toIterable().hasSize(1000);
    }

    private S3AsyncClient createS3AsyncClient() {
        return buildAwsV2Client(localStackContainer, LocalStackContainer.Service.S3, S3AsyncClient.builder());
    }
}
