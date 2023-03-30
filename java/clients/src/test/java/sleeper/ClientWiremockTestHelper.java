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
package sleeper;

import com.amazonaws.services.cloudwatchevents.AmazonCloudWatchEvents;
import com.amazonaws.services.cloudwatchevents.AmazonCloudWatchEventsClient;
import com.amazonaws.services.ecr.AmazonECR;
import com.amazonaws.services.ecr.AmazonECRClient;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;

import static sleeper.job.common.WiremockTestHelper.wiremockCredentialsProvider;
import static sleeper.job.common.WiremockTestHelper.wiremockEndpointConfiguration;

public class ClientWiremockTestHelper {

    private ClientWiremockTestHelper() {
    }

    public static AmazonECR wiremockEcrClient(WireMockRuntimeInfo runtimeInfo) {
        return AmazonECRClient.builder()
                .withEndpointConfiguration(wiremockEndpointConfiguration(runtimeInfo))
                .withCredentials(wiremockCredentialsProvider())
                .build();
    }

    public static AmazonCloudWatchEvents wiremockCloudWatchClient(WireMockRuntimeInfo runtimeInfo) {
        return AmazonCloudWatchEventsClient.builder()
                .withEndpointConfiguration(wiremockEndpointConfiguration(runtimeInfo))
                .withCredentials(wiremockCredentialsProvider())
                .build();
    }
}
