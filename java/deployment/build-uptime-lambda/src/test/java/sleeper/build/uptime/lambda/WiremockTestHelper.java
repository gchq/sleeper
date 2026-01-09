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
package sleeper.build.uptime.lambda;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;

import java.net.URI;

public class WiremockTestHelper {

    public static final String WIREMOCK_ACCESS_KEY = "wiremock-access-key";
    public static final String WIREMOCK_SECRET_KEY = "wiremock-secret-key";

    private WiremockTestHelper() {
    }

    public static <B extends AwsClientBuilder<B, T>, T> T wiremockClient(WireMockRuntimeInfo runtimeInfo, B builder) {
        return builder
                .endpointOverride(URI.create(runtimeInfo.getHttpBaseUrl()))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(WIREMOCK_ACCESS_KEY, WIREMOCK_SECRET_KEY)))
                .build();
    }
}
