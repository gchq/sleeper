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

package sleeper.configuration.properties.local;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.local.LoadLocalProperties.loadInstanceProperties;
import static sleeper.configuration.properties.local.LoadLocalProperties.loadTablesFromInstancePropertiesFile;
import static sleeper.configuration.properties.local.SaveLocalProperties.saveFromS3;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

@Testcontainers
class SaveLocalPropertiesIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    private final AmazonS3 s3Client = createS3Client();
    @TempDir
    private Path tempDir;

    private AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
                        localStackContainer.getRegion()))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                        localStackContainer.getAccessKey(), localStackContainer.getSecretKey())))
                .build();
    }

    @Test
    void shouldLoadInstancePropertiesFromS3() throws IOException {
        // Given
        InstanceProperties properties = createTestInstanceProperties(s3Client);

        // When
        saveFromS3(s3Client, properties.get(ID), tempDir);

        // Then
        assertThat(loadInstanceProperties(new InstanceProperties(), tempDir.resolve("instance.properties")))
                .isEqualTo(properties);
    }

    @Test
    void shouldLoadTablePropertiesFromS3() throws IOException {
        // Given
        InstanceProperties properties = createTestInstanceProperties(s3Client);
        TableProperties table1 = createTestTableProperties(properties, schemaWithKey("key1"), s3Client);
        TableProperties table2 = createTestTableProperties(properties, schemaWithKey("key2"), s3Client);

        // When
        saveFromS3(s3Client, properties.get(ID), tempDir);

        // Then
        assertThat(loadTablesFromInstancePropertiesFile(properties, tempDir.resolve("instance.properties")))
                .containsExactlyInAnyOrder(table1, table2);
    }

    @Test
    void shouldLoadNoTablePropertiesFromS3WhenNoneAreSaved() throws IOException {
        // Given
        InstanceProperties properties = createTestInstanceProperties(s3Client);

        // When
        saveFromS3(s3Client, properties.get(ID), tempDir);

        // Then
        assertThat(loadTablesFromInstancePropertiesFile(properties, tempDir.resolve("instance.properties"))).isEmpty();
    }

    @Test
    void shouldLoadAndReturnInstancePropertiesFromS3() throws IOException {
        // Given
        InstanceProperties properties = createTestInstanceProperties(s3Client);

        // When
        InstanceProperties saved = saveFromS3(s3Client, properties.get(ID), tempDir);

        // Then
        assertThat(properties).isEqualTo(saved);
    }
}
