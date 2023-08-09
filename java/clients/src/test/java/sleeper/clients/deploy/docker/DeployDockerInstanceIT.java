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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.clients.docker.DeployDockerInstance;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.StateStoreProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class DeployDockerInstanceIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);
    private final AmazonS3 s3Client = buildAwsV1Client(
            localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(
            localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());

    @Test
    void shouldDeployInstance() throws Exception {
        // Given
        String instanceId = "test-instance";

        // When
        DeployDockerInstance.deploy(instanceId, s3Client, dynamoDB);

        // Then
        InstanceProperties instanceProperties = new InstanceProperties();
        assertThatCode(() -> instanceProperties.loadFromS3GivenInstanceId(s3Client, "test-instance"))
                .doesNotThrowAnyException();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        assertThatCode(() -> tableProperties.loadFromS3(s3Client, "system-test"))
                .doesNotThrowAnyException();
        StateStore stateStore = new StateStoreProvider(dynamoDB, instanceProperties).getStateStore(tableProperties);
        assertThat(stateStore.getActiveFiles()).isEmpty();
        assertThat(stateStore.getReadyForGCFiles()).isExhausted();
        assertThat(stateStore.getAllPartitions())
                .isEqualTo(new PartitionsBuilder(tableProperties.getSchema())
                        .rootFirst("root")
                        .buildList());
    }
}
