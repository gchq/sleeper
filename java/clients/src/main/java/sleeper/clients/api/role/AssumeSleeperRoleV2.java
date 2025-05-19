/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.clients.api.role;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;

import java.net.URI;
import java.util.Map;

public class AssumeSleeperRoleV2 {
    private final String region;
    private final String endpointUrl;
    private final AwsCredentialsProvider provider;

    AssumeSleeperRoleV2(String region, String endpointUrl, AwsCredentialsProvider provider) {
        this.region = region;
        this.endpointUrl = endpointUrl;
        this.provider = provider;
    }

    public <T, B extends software.amazon.awssdk.awscore.client.builder.AwsClientBuilder<B, T>> T buildClient(B builder) {
        builder.credentialsProvider(provider).region(Region.of(region));
        if (endpointUrl != null) {
            builder.endpointOverride(URI.create(endpointUrl));
        }
        return builder.build();
    }

    public S3AsyncClient buildClient(S3CrtAsyncClientBuilder builder) {
        builder.credentialsProvider(provider).region(Region.of(region));
        if (endpointUrl != null) {
            builder.endpointOverride(URI.create(endpointUrl));
        }
        return builder.build();
    }

    public AwsRegionProvider regionProvider() {
        return () -> Region.of(region);
    }

    public Map<String, String> authEnvVars() {
        AwsSessionCredentials credentials = (AwsSessionCredentials) provider.resolveCredentials();
        return Map.of(
                "AWS_ACCESS_KEY_ID", credentials.accessKeyId(),
                "AWS_SECRET_ACCESS_KEY", credentials.secretAccessKey(),
                "AWS_SESSION_TOKEN", credentials.sessionToken(),
                "AWS_REGION", region,
                "AWS_DEFAULT_REGION", region);
    }
}
