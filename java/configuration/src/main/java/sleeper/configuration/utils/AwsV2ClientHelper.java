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
package sleeper.configuration.utils;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;

import java.net.URI;

/**
 * Applies default configuration to AWS SDK v2 clients.
 */
public class AwsV2ClientHelper {
    private static final String AWS_ENDPOINT_ENV_VAR = "AWS_ENDPOINT_URL";

    private AwsV2ClientHelper() {
    }

    /**
     * Builds an AWS SDK v2 client with default configuration.
     *
     * @param  <B>     the builder type
     * @param  <T>     the client type
     * @param  builder the builder
     * @return         the client
     */
    public static <B extends AwsClientBuilder<B, T>, T> T buildAwsV2Client(B builder) {
        String endpoint = System.getenv(AWS_ENDPOINT_ENV_VAR);
        if (endpoint != null) {
            if (builder instanceof S3BaseClientBuilder) {
                ((S3BaseClientBuilder<?, ?>) builder).forcePathStyle(true);
            }
            return builder
                    .endpointOverride(URI.create(endpoint))
                    .region(Region.US_EAST_1)
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("test-access-key", "test-secret-key")))
                    .build();
        } else {
            return builder.build();
        }
    }

}
