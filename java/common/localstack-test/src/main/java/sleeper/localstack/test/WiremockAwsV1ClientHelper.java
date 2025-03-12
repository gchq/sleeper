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

package sleeper.localstack.test;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.AwsSyncClientBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;

import static com.amazonaws.regions.Regions.DEFAULT_REGION;

public class WiremockAwsV1ClientHelper {

    public static final String WIREMOCK_ACCESS_KEY = "wiremock-access-key";
    public static final String WIREMOCK_SECRET_KEY = "wiremock-secret-key";

    private WiremockAwsV1ClientHelper() {
    }

    public static <C, B extends AwsSyncClientBuilder<B, C>> C buildAwsV1Client(WireMockRuntimeInfo runtimeInfo, B builder) {
        return builder
                .withEndpointConfiguration(wiremockEndpointConfiguration(runtimeInfo))
                .withCredentials(wiremockCredentialsProvider())
                .build();
    }

    private static AwsClientBuilder.EndpointConfiguration wiremockEndpointConfiguration(WireMockRuntimeInfo runtimeInfo) {
        return new AwsClientBuilder.EndpointConfiguration(runtimeInfo.getHttpBaseUrl(), DEFAULT_REGION.getName());
    }

    private static AWSCredentialsProvider wiremockCredentialsProvider() {
        return new AWSStaticCredentialsProvider(new BasicAWSCredentials(WIREMOCK_ACCESS_KEY, WIREMOCK_SECRET_KEY));
    }
}
