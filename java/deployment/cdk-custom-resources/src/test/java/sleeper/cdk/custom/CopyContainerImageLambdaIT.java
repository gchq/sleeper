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
package sleeper.cdk.custom;

import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent.CloudFormationCustomResourceEventBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import sleeper.cdk.custom.testutil.FakeLambdaContext;
import sleeper.core.deploy.ContainerPlatform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@WireMockTest
public class CopyContainerImageLambdaIT {

    private final Map<String, String> imageReferenceToDigest = new HashMap<>();
    private final Map<String, List<ContainerPlatform>> transferredPlatforms = new HashMap<>();

    @BeforeEach
    void setUp() {
        stubFor(put("/report-response").willReturn(aResponse().withStatus(200)));
    }

    @Test
    void shouldCopyDockerImageOnCreate(WireMockRuntimeInfo runtimeInfo) throws Exception {
        // Given
        imageReferenceToDigest.put("source-registry/test-image", "test-digest");

        // When
        handleEvent(event(runtimeInfo)
                .withRequestType("Create")
                .withResourceProperties(Map.of(
                        "source", "source-registry/test-image",
                        "target", "target-registry/test-image"))
                .build());

        // Then
        assertThat(imageReferenceToDigest).isEqualTo(Map.of(
                "source-registry/test-image", "test-digest",
                "target-registry/test-image", "test-digest"));
        verify(putRequestedFor(urlEqualTo("/report-response"))
                .withRequestBody(matchingJsonPath("$.Data.digest", equalTo("test-digest"))
                        .and(matchingJsonPath("$.Status", equalTo("SUCCESS")))));
    }

    @Test
    void shouldCopyNewDockerImageOnUpdate(WireMockRuntimeInfo runtimeInfo) throws Exception {
        // Given
        imageReferenceToDigest.put("source-registry/test-image:1.1", "new-digest");
        imageReferenceToDigest.put("target-registry/test-image:1.0", "old-digest");

        // When
        handleEvent(event(runtimeInfo)
                .withRequestType("Update")
                .withResourceProperties(Map.of(
                        "source", "source-registry/test-image:1.1",
                        "target", "target-registry/test-image:1.1"))
                .withOldResourceProperties(Map.of(
                        "source", "source-registry/test-image:1.0",
                        "target", "target-registry/test-image:1.0"))
                .build());

        // Then
        assertThat(imageReferenceToDigest).isEqualTo(Map.of(
                "source-registry/test-image:1.1", "new-digest",
                "target-registry/test-image:1.0", "old-digest",
                "target-registry/test-image:1.1", "new-digest"));
        verify(putRequestedFor(urlEqualTo("/report-response"))
                .withRequestBody(matchingJsonPath("$.Data.digest", equalTo("new-digest"))
                        .and(matchingJsonPath("$.Status", equalTo("SUCCESS")))));
    }

    @Test
    void shouldForwardPlatformsToTransfer(WireMockRuntimeInfo runtimeInfo) throws Exception {
        // Given
        imageReferenceToDigest.put("source-registry/multi-image", "multi-digest");

        // When
        handleEvent(event(runtimeInfo)
                .withRequestType("Create")
                .withResourceProperties(Map.of(
                        "source", "source-registry/multi-image",
                        "target", "target-registry/multi-image",
                        "platforms", "linux/amd64,linux/arm64"))
                .build());

        // Then
        assertThat(transferredPlatforms).isEqualTo(Map.of(
                "target-registry/multi-image", List.of(ContainerPlatform.LINUX_AMD64, ContainerPlatform.LINUX_ARM64)));
    }

    @Test
    void shouldTransferWithNoPlatformsWhenPropertyIsEmpty(WireMockRuntimeInfo runtimeInfo) throws Exception {
        // Given
        imageReferenceToDigest.put("source-registry/test-image", "test-digest");

        // When
        handleEvent(event(runtimeInfo)
                .withRequestType("Create")
                .withResourceProperties(Map.of(
                        "source", "source-registry/test-image",
                        "target", "target-registry/test-image",
                        "platforms", ""))
                .build());

        // Then
        assertThat(transferredPlatforms).isEqualTo(Map.of(
                "target-registry/test-image", List.of()));
    }

    @Test
    void shouldDoNothingOnDelete(WireMockRuntimeInfo runtimeInfo) throws Exception {
        // When
        handleEvent(event(runtimeInfo)
                .withRequestType("Delete")
                .withResourceProperties(Map.of(
                        "source", "source-registry/test-image:1.0",
                        "target", "target-registry/test-image:1.0"))
                .build());

        // Then
        verify(putRequestedFor(urlEqualTo("/report-response"))
                .withRequestBody(equalToJson("{\"Status\":\"SUCCESS\",\"Data\":null}", true, true)));
        assertThat(imageReferenceToDigest).isEmpty();
    }

    private void handleEvent(CloudFormationCustomResourceEvent event) throws Exception {
        lambda().handleRequest(event, new FakeLambdaContext());
    }

    private CloudFormationCustomResourceEventBuilder event(WireMockRuntimeInfo runtimeInfo) {
        return CloudFormationCustomResourceEvent.builder()
                .withResponseUrl(runtimeInfo.getHttpBaseUrl() + "/report-response");
    }

    private CopyContainerImageLambda lambda() {
        return new CopyContainerImageLambda((source, target, platforms) -> {
            transferredPlatforms.put(target, platforms);
            String digest = imageReferenceToDigest.get(source);
            imageReferenceToDigest.put(target, digest);
            return digest;
        });
    }

}
