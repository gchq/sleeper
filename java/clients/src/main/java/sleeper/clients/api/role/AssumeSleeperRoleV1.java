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

import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSSessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;

import java.util.Map;

public class AssumeSleeperRoleV1 {

    private final String region;
    private final String endpointUrl;
    private final AWSSessionCredentialsProvider provider;

    AssumeSleeperRoleV1(String region, String endpointUrl, AWSSessionCredentialsProvider provider) {
        this.region = region;
        this.endpointUrl = endpointUrl;
        this.provider = provider;
    }

    public <T, B extends com.amazonaws.client.builder.AwsClientBuilder<B, T>> T buildClient(B builder) {
        builder.withCredentials(provider);
        if (endpointUrl != null) {
            builder.withEndpointConfiguration(new EndpointConfiguration(endpointUrl, region));
        } else {
            builder.withRegion(region);
        }
        return builder.build();
    }

    public Map<String, String> authEnvVars() {
        AWSSessionCredentials credentials = provider.getCredentials();
        return Map.of(
                "AWS_ACCESS_KEY_ID", credentials.getAWSAccessKeyId(),
                "AWS_SECRET_ACCESS_KEY", credentials.getAWSSecretKey(),
                "AWS_SESSION_TOKEN", credentials.getSessionToken(),
                "AWS_REGION", region,
                "AWS_DEFAULT_REGION", region);
    }

}
