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
package sleeper.cdk;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.CommonTestConstants;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.cdk.ValidatorTestHelper.setupTablesPropertiesFile;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;

@Testcontainers
public class ConfigValidatorIT {

    @Container
    public static final LocalStackContainer LOCALSTACK_CONTAINER
            = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    @TempDir
    public Path temporaryFolder;

    private static AmazonS3 amazonS3;
    private static AmazonDynamoDB amazonDynamoDB;
    private ConfigValidator configValidator;
    private final InstanceProperties instanceProperties = new InstanceProperties();

    @BeforeAll
    public static void setup() {
        amazonS3 = getS3Client();
        amazonDynamoDB = createDynamoClient();
    }

    @BeforeEach
    public void setUp() {
        configValidator = new ConfigValidator(amazonS3, amazonDynamoDB);
    }

    @Test
    public void shouldNotThrowAnErrorWithValidConfiguration() throws IOException {
        // Given
        instanceProperties.set(ID, "valid-id");
        setupTablesPropertiesFile(temporaryFolder, "example-valid-table", "sleeper.statestore.dynamodb.DynamoDBStateStore");

        // When / Then
        assertThatCode(this::validate)
                .doesNotThrowAnyException();
    }


    @Test
    public void shouldThrowAnErrorWhenTableNameIsNotValid() throws IOException {
        // Given
        instanceProperties.set(ID, "valid-id");
        setupTablesPropertiesFile(temporaryFolder, "example--invalid-name-tab$$-le", "sleeper.statestore.dynamodb.DynamoDBStateStore");

        // When / Then
        assertThatThrownBy(this::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Sleeper table bucket name is illegal: sleeper-valid-id-table-example--invalid-name-tab$$-le");
    }

    @Test
    public void shouldThrowAnErrorWithAnInvalidSleeperId() {
        // Given
        instanceProperties.set(ID, "aa$$aa");

        // When / Then
        assertThatThrownBy(this::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Sleeper instance id is illegal: aa$$aa");
    }


    private void validate() throws IOException {
        Path instancePropertiesPath = temporaryFolder.resolve("instance.properties");
        Files.writeString(instancePropertiesPath, instanceProperties.saveAsString());
        configValidator.validate(instanceProperties, instancePropertiesPath);
    }

    private static AmazonS3 getS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(LOCALSTACK_CONTAINER.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(LOCALSTACK_CONTAINER.getDefaultCredentialsProvider())
                .build();
    }

    protected static AmazonDynamoDB createDynamoClient() {
        return AmazonDynamoDBClient.builder()
                .withEndpointConfiguration(LOCALSTACK_CONTAINER.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB))
                .withCredentials(LOCALSTACK_CONTAINER.getDefaultCredentialsProvider())
                .build();
    }
}
