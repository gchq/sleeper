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
package sleeper.cdk.custom.containers;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.google.cloud.tools.jib.api.Credential;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.ecr.EcrClient;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.findUnmatchedRequests;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2Client;

@WireMockTest
public class EcrCredentialRetrieverIT {

    @Test
    void shouldRetrieveCredentials(WireMockRuntimeInfo runtimeInfo) throws Exception {
        // Given
        stubFor(post("/")
                .withHeader("X-Amz-Target", equalTo("AmazonEC2ContainerRegistry_V20150921.GetAuthorizationToken"))
                .withRequestBody(equalTo("{}"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody("""
                                {
                                    "authorizationData": [
                                        {
                                            "authorizationToken": "dGVzdC11c2VyOnRlc3QtcGFzc3dvcmQ=",
                                            "expiresAt": 123456,
                                            "proxyEndpoint": "ignored"
                                        }
                                    ]
                                }
                                """)));

        // When / Then
        assertThat(credentialRetriever(runtimeInfo).retrieve())
                .get().isEqualTo(Credential.from("test-user", "test-password"));
        assertThat(findUnmatchedRequests()).isEmpty();
    }

    private EcrCredentialRetriever credentialRetriever(WireMockRuntimeInfo runtimeInfo) {
        return new EcrCredentialRetriever(wiremockAwsV2Client(runtimeInfo, EcrClient.builder()));
    }

}
