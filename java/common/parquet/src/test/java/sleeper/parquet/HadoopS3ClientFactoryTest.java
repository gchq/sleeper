/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3ClientFactory.S3ClientCreationParameters;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;

import static org.apache.hadoop.fs.s3a.Constants.REQUEST_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

class HadoopS3ClientFactoryTest {

    private static final URI TEST_URI = URI.create("s3a://test-bucket/");

    private final Configuration conf = new Configuration();
    private final HadoopS3ClientFactory factory = new HadoopS3ClientFactory();

    @BeforeEach
    void setUp() {
        factory.setConf(conf);
    }

    @Test
    void shouldApplyHadoopConfiguration() throws Exception {
        // When
        HadoopS3ClientFactory.configureHadoop(conf);

        // Then
        assertThat(conf.get("fs.s3a.s3.client.factory.impl"))
                .isEqualTo(HadoopS3ClientFactory.class.getName());
    }

    @Test
    void shouldGenerateEndpointFromDefaultParamters() throws Exception {
        // Given
        S3ClientCreationParameters parameters = createParameters().withEndpoint("s3.example.com:8080");

        // When
        S3ServiceClientConfiguration configuration = createClientConfiguration(parameters);

        // Then
        assertThat(configuration.endpointOverride())
                .contains(URI.create("https://s3.example.com:8080"));
    }

    @Test
    void shouldApplyRequestTimeoutFromConfiguration() throws Exception {
        // Given
        conf.set(REQUEST_TIMEOUT, "5m");

        // When
        S3ServiceClientConfiguration configuration = createClientConfiguration(createParameters());

        // Then
        assertThat(configuration.overrideConfiguration().apiCallTimeout())
                .contains(Duration.ofMinutes(5));
    }

    private S3ServiceClientConfiguration createClientConfiguration(S3ClientCreationParameters parameters) throws IOException {
        try (S3Client client = factory.createS3Client(TEST_URI, parameters)) {
            return client.serviceClientConfiguration();
        }
    }

    private static S3ClientCreationParameters createParameters() {
        AwsCredentialsProvider credentials = StaticCredentialsProvider.create(
                AwsBasicCredentials.create("test-access-key", "test-secret-key"));
        return new S3ClientCreationParameters()
                .withCredentialSet(credentials);
    }
}
