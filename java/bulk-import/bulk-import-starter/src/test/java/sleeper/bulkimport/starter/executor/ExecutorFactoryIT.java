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
package sleeper.bulkimport.starter.executor;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.core.CommonTestConstants;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;

@Testcontainers
public class ExecutorFactoryIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    private final Map<String, String> environment = new HashMap<>();

    private static final String DEFAULT_CONFIGURATION_FILE = "" +
            "sleeper.id=basic-example\n" +
            "sleeper.region=eu-west-2\n" +
            "sleeper.version=0.10.0-SNAPSHOT\n" +
            "sleeper.jars.bucket=jars-bucket\n" +
            "sleeper.vpc=1234567890\n" +
            "sleeper.subnet=subnet-123\n" +
            "sleeper.account=123\n";

    @BeforeEach
    public void setUp() {
        environment.put(CONFIG_BUCKET.toEnvironmentVariable(), "config-bucket");
    }

    private ExecutorFactory executorFactory(AmazonS3 s3Client) throws IOException {
        return new ExecutorFactory(s3Client,
                mock(AmazonElasticMapReduceClient.class), mock(AWSStepFunctionsClient.class), mock(AmazonDynamoDB.class),
                "test-task", Instant::now, environment::get);
    }

    @Test
    public void shouldCreateEmrExecutor() throws IOException {
        // Given
        AmazonS3 s3Client = createS3Client();
        s3Client.createBucket("config-bucket");
        s3Client.putObject("config-bucket", "config", DEFAULT_CONFIGURATION_FILE);
        setEnvironmentVariable("BULK_IMPORT_PLATFORM", "NonPersistentEMR");
        ExecutorFactory executorFactory = executorFactory(s3Client);

        // When
        Executor executor = executorFactory.createExecutor();

        // Then
        assertThat(executor).isInstanceOf(EmrExecutor.class);

        s3Client.shutdown();
    }

    @Test
    public void shouldCreateStateMachineExecutor() throws IOException {
        // Given
        String configurationFile = DEFAULT_CONFIGURATION_FILE;
        AmazonS3 s3Client = createS3Client();
        s3Client.createBucket("config-bucket");
        s3Client.putObject("config-bucket", "config", configurationFile);
        setEnvironmentVariable("BULK_IMPORT_PLATFORM", "EKS");
        ExecutorFactory executorFactory = executorFactory(s3Client);

        // When
        Executor executor = executorFactory.createExecutor();

        // Then
        assertThat(executor).isInstanceOf(StateMachineExecutor.class);

        s3Client.shutdown();
    }

    @Test
    public void shouldNotCreateExecutorWithInvalidConfiguration() throws IOException {
        // Given
        String configurationFile = DEFAULT_CONFIGURATION_FILE;
        setEnvironmentVariable(CONFIG_BUCKET.toEnvironmentVariable(), "config-bucket");
        AmazonS3 s3Client = createS3Client();
        s3Client.createBucket("config-bucket");
        s3Client.putObject("config-bucket", "config", configurationFile);
        setEnvironmentVariable("BULK_IMPORT_PLATFORM", "ZZZ");
        ExecutorFactory executorFactory = executorFactory(s3Client);

        // When then
        assertThatThrownBy(executorFactory::createExecutor).isInstanceOf(IllegalArgumentException.class);

        s3Client.shutdown();
    }

    public void setEnvironmentVariable(String key, String value) {
        environment.put(key, value);
    }

    private AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }
}
