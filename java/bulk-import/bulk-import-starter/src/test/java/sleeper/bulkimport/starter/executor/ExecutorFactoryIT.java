/*
 * Copyright 2022 Crown Copyright
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

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClient;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import sleeper.core.CommonTestConstants;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;

public class ExecutorFactoryIT {
    @ClassRule
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    private String previousConfigBucketEnvSetting;
    private String previousBulkImportPlatformEnvSetting;

    private static final String DEFAULT_CONFIGURATION_FILE = "" +
            "sleeper.id=basic-example\n" +
            "sleeper.table.properties=example/basic/table.properties\n" +
            "sleeper.region=eu-west-2\n" +
            "sleeper.version=0.10.0-SNAPSHOT\n" +
            "sleeper.jars.bucket=jars-bucket\n" +
            "sleeper.vpc=1234567890\n" +
            "sleeper.subnet=subnet-123\n" +
            "sleeper.account=123\n";

    @Before
    public void setUp() {
        previousConfigBucketEnvSetting = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        previousBulkImportPlatformEnvSetting = System.getenv("BULK_IMPORT_PLATFORM");
        setEnvironmentVariable(CONFIG_BUCKET.toEnvironmentVariable(), "config-bucket");
    }

    @After
    public void cleanUp() {
        if (null == previousConfigBucketEnvSetting) {
            previousConfigBucketEnvSetting = "";
        }
        setEnvironmentVariable(CONFIG_BUCKET.toEnvironmentVariable(), previousConfigBucketEnvSetting);
        if (null == previousBulkImportPlatformEnvSetting) {
            previousBulkImportPlatformEnvSetting = "";
        }
        setEnvironmentVariable("BULK_IMPORT_PLATFORM", previousBulkImportPlatformEnvSetting);
    }

    @Test
    public void shouldCreateEmrExecutor() throws IOException {
        // Given
        AmazonS3 s3Client = createS3Client();
        s3Client.createBucket("config-bucket");
        s3Client.putObject("config-bucket", "config", DEFAULT_CONFIGURATION_FILE);
        setEnvironmentVariable("BULK_IMPORT_PLATFORM", "NonPersistentEMR");
        ExecutorFactory executorFactory = new ExecutorFactory(s3Client, mock(AmazonElasticMapReduceClient.class), mock(AWSStepFunctionsClient.class));

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
        ExecutorFactory executorFactory = new ExecutorFactory(s3Client, mock(AmazonElasticMapReduceClient.class), mock(AWSStepFunctionsClient.class));

        // When
        Executor executor = executorFactory.createExecutor();

        // Then
        assertThat(executor).isInstanceOf(StateMachineExecutor.class);

        s3Client.shutdown();
    }

    @Test
    public void shouldNotCreateExecutorWithInvalidConfiguration() {
        // Given
        String configurationFile = DEFAULT_CONFIGURATION_FILE;
        setEnvironmentVariable(CONFIG_BUCKET.toEnvironmentVariable(), "config-bucket");
        AmazonS3 s3Client = createS3Client();
        s3Client.createBucket("config-bucket");
        s3Client.putObject("config-bucket", "config", configurationFile);
        setEnvironmentVariable("BULK_IMPORT_PLATFORM", "ZZZ");

        // When then
        assertThrows(IllegalArgumentException.class, () -> {
            ExecutorFactory executorFactory = new ExecutorFactory(s3Client, mock(AmazonElasticMapReduceClient.class), mock(AWSStepFunctionsClient.class));
            executorFactory.createExecutor();
        });

        s3Client.shutdown();
    }

    public void setEnvironmentVariable(String key, String value) {
        try {
            Map<String, String> env = System.getenv();
            Class<?> cl = env.getClass();
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> writableEnv;
            writableEnv = (Map<String, String>) field.get(env);
            writableEnv.put(key, value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set environment variable", e);
        }
    }

    private AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }
}
