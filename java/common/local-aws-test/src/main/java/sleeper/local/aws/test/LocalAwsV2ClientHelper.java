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
package sleeper.local.aws.test;

import io.floci.testcontainers.FlociContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;

import java.net.URI;

public class LocalAwsV2ClientHelper {

    private LocalAwsV2ClientHelper() {
    }

    public static <B extends AwsClientBuilder<B, T>, T> T buildAwsV2Client(FlociContainer localAwsContainer, B builder) {
        return builder
                .endpointOverride(URI.create(localAwsContainer.getEndpoint()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        localAwsContainer.getAccessKey(), localAwsContainer.getSecretKey())))
                .region(Region.of(localAwsContainer.getRegion()))
                .build();
    }

    public static S3AsyncClient buildAwsV2Client(FlociContainer localAwsContainer, S3CrtAsyncClientBuilder builder) {
        return builder
                .endpointOverride(URI.create(localAwsContainer.getEndpoint()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        localAwsContainer.getAccessKey(), localAwsContainer.getSecretKey())))
                .region(Region.of(localAwsContainer.getRegion()))
                .build();
    }

    public static AwsCredentialsProvider buildAwsCredentialsProvider() {
        return StaticCredentialsProvider.create(
                AwsBasicCredentials.create("test", "test"));
    }

}
