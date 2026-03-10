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

import sleeper.foreign.datafusion.DataFusionAwsConfig;

import java.net.URI;
import java.util.Map;

/**
 * A factory for AWS SDK clients that will assume a role. Created with {@link AssumeSleeperRole}.
 */
public class AssumeSleeperRoleAwsSdk {
    private final String region;
    private final String endpointUrl;
    private final AwsCredentialsProvider credentialsProvider;

    AssumeSleeperRoleAwsSdk(String region, String endpointUrl, AwsCredentialsProvider provider) {
        this.region = region;
        this.endpointUrl = endpointUrl;
        this.credentialsProvider = provider;
    }

    /**
     * Configures and creates an AWS SDK client to assume the role.
     *
     * @param  <T>     the type of AWS client
     * @param  <B>     the type of AWS client builder
     * @param  builder the AWS client builder
     * @return         the AWS client
     */
    public <T, B extends software.amazon.awssdk.awscore.client.builder.AwsClientBuilder<B, T>> T buildClient(B builder) {
        builder.credentialsProvider(credentialsProvider).region(Region.of(region));
        if (endpointUrl != null) {
            builder.endpointOverride(URI.create(endpointUrl));
        }
        return builder.build();
    }

    /**
     * Configures and creates a Common Run Time based S3AsyncClient to assume the role. This method is needed because
     * the CRT client builder does not implement {@link software.amazon.awssdk.awscore.client.builder.AwsClientBuilder}.
     *
     * @param  builder the AWS client builder
     * @return         the AWS client
     */
    public S3AsyncClient buildClient(S3CrtAsyncClientBuilder builder) {
        builder.credentialsProvider(credentialsProvider).region(Region.of(region));
        if (endpointUrl != null) {
            builder.endpointOverride(URI.create(endpointUrl));
        }
        return builder.build();
    }

    /**
     * Returns a region provider to set the region associated with the role.
     *
     * @return the region provider
     */
    public AwsRegionProvider regionProvider() {
        return () -> Region.of(region);
    }

    /**
     * Returns a credentials provider that assumes the role.
     *
     * @return the credentials provider
     */
    public AwsCredentialsProvider credentialsProvider() {
        return credentialsProvider;
    }

    /**
     * Generates environment variables that will configure AWS tools to assume the role. This will not refresh
     * credentials automatically, so please use {@link #credentialsProvider()} when possible.
     *
     * @return the environment variables
     */
    public Map<String, String> authEnvVars() {
        AwsSessionCredentials credentials = (AwsSessionCredentials) credentialsProvider.resolveCredentials();
        return Map.of(
                "AWS_ACCESS_KEY_ID", credentials.accessKeyId(),
                "AWS_SECRET_ACCESS_KEY", credentials.secretAccessKey(),
                "AWS_SESSION_TOKEN", credentials.sessionToken(),
                "AWS_REGION", region,
                "AWS_DEFAULT_REGION", region);
    }

    /**
     * Generates configuration for DataFusion to assume the role. This will not refresh credentials automatically, so
     * please configure this in DataFusion directly when possible.
     *
     * @return the configuration
     */
    public DataFusionAwsConfig dataFusionAwsConfig() {
        AwsSessionCredentials credentials = (AwsSessionCredentials) credentialsProvider.resolveCredentials();
        return DataFusionAwsConfig.builder()
                .accessKey(credentials.accessKeyId())
                .secretKey(credentials.secretAccessKey())
                .sessionToken(credentials.sessionToken())
                .region(region)
                .build();
    }
}
