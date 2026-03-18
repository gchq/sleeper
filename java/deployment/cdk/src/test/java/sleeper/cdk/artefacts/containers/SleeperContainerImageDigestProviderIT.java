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
package sleeper.cdk.artefacts.containers;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.ImageNotFoundException;
import software.amazon.awssdk.services.ecr.model.RepositoryNotFoundException;

import sleeper.core.deploy.DockerDeployment;
import sleeper.core.properties.instance.InstanceProperties;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.findUnmatchedRequests;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2Client;

@WireMockTest
public class SleeperContainerImageDigestProviderIT {

    private final InstanceProperties instanceProperties = createInstanceProperties();

    @Test
    void shouldGetLatestDigestOfAnImage(WireMockRuntimeInfo runtimeInfo) throws Exception {

        // Given
        String expectedDigest = "sha256:abc123...";
        stubFor(post("/")
                .withHeader("X-Amz-Target", equalTo("AmazonEC2ContainerRegistry_V20150921.DescribeImages"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/x-amz-json-1.1")
                        .withBody("""
                                {
                                    "imageDetails": [
                                        {
                                            "imageDigest": "sha256:abc123...",
                                            "imageTags": ["latest"]
                                        }
                                    ]
                                }
                                """)));

        // When / Then
        assertThat(digestProvider(runtimeInfo).getDigestForLatestVersion(DockerDeployment.INGEST))
                .isEqualTo(expectedDigest);
        assertThat(findUnmatchedRequests()).isEmpty();
    }

    @Test
    void shouldThrowAnErrorIfImageNotFound(WireMockRuntimeInfo runtimeInfo) throws Exception {

        // Given
        stubFor(post("/")
                .withHeader("X-Amz-Target", equalTo("AmazonEC2ContainerRegistry_V20150921.DescribeImages"))
                .willReturn(aResponse()
                        .withStatus(400)
                        .withHeader("Content-Type", "application/x-amz-json-1.1")
                        .withHeader("x-amzn-ErrorType", "ImageNotFoundException")
                        .withBody("""
                                {
                                    "__type": "ImageNotFoundException",
                                    "message": "Image not found."
                                }
                                """)));

        // When / Then
        assertThatThrownBy(() -> digestProvider(runtimeInfo).getDigestForLatestVersion(DockerDeployment.INGEST))
                .isInstanceOf(ImageNotFoundException.class)
                .hasMessageContaining("Image not found");
        assertThat(findUnmatchedRequests()).isEmpty();
    }

    @Test
    void shouldThrowAnErrorIfRepositoryNotFound(WireMockRuntimeInfo runtimeInfo) throws Exception {

        // Given
        stubFor(post("/")
                .withHeader("X-Amz-Target", equalTo("AmazonEC2ContainerRegistry_V20150921.DescribeImages"))
                .willReturn(aResponse()
                        .withStatus(400)
                        .withHeader("Content-Type", "application/x-amz-json-1.1")
                        .withHeader("x-amzn-ErrorType", "RepositoryNotFoundException")
                        .withBody("""
                                {
                                    "__type": "RepositoryNotFoundException",
                                    "message": "Repository not found."
                                }
                                """)));

        // When / Then
        assertThatThrownBy(() -> digestProvider(runtimeInfo).getDigestForLatestVersion(DockerDeployment.INGEST))
                .isInstanceOf(RepositoryNotFoundException.class)
                .hasMessageContaining("Repository not found");
        assertThat(findUnmatchedRequests()).isEmpty();
    }

    private InstanceProperties createInstanceProperties() {
        InstanceProperties properties = createTestInstanceProperties();
        return properties;
    }

    private SleeperContainerImageDigestProvider digestProvider(WireMockRuntimeInfo runtimeInfo) {
        return SleeperContainerImageDigestProvider.from(wiremockAwsV2Client(runtimeInfo, EcrClient.builder()), instanceProperties);
    }
}
