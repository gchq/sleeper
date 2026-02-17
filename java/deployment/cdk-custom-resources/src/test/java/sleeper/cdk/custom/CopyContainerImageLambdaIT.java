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
package sleeper.cdk.custom;

import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent.CloudFormationCustomResourceEventBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.google.cloud.tools.jib.api.Containerizer;
import com.google.cloud.tools.jib.api.Jib;
import com.google.cloud.tools.jib.api.RegistryImage;
import com.google.cloud.tools.jib.event.EventHandlers;
import com.google.cloud.tools.jib.http.FailoverHttpClient;
import com.google.cloud.tools.jib.image.json.V22ManifestTemplate;
import com.google.cloud.tools.jib.registry.ManifestAndDigest;
import com.google.cloud.tools.jib.registry.RegistryClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.cdk.custom.containers.JibEvents;
import sleeper.cdk.custom.testutil.FakeLambdaContext;

import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(CopyContainerImageLambdaIT.class);

    @Container
    public GenericContainer<?> source = createDockerRegistryContainer();

    @Container
    public GenericContainer<?> destination = createDockerRegistryContainer();

    @BeforeEach
    void setUp() {
        stubFor(put("/report-response").willReturn(aResponse().withStatus(200)));
    }

    @Test
    void shouldCopyDockerImageOnCreate(WireMockRuntimeInfo runtimeInfo) throws Exception {
        // Given
        copyImage("scratch", imageNameInRegistry(source, "test"));

        // When
        handleEvent(event(runtimeInfo)
                .withRequestType("Create")
                .withResourceProperties(Map.of(
                        "source", imageNameInRegistry(source, "test"),
                        "target", imageNameInRegistry(destination, "test")))
                .build());

        // Then
        assertThat(registryClient(destination, "test").pullManifest("latest"))
                .extracting(ManifestAndDigest::getManifest)
                .isInstanceOf(V22ManifestTemplate.class);
        verify(putRequestedFor(urlEqualTo("/report-response"))
                .withRequestBody(matchingJsonPath("$.Data.digest", matching("sha256:[a-z0-9]+"))
                        .and(matchingJsonPath("$.Status", equalTo("SUCCESS")))));
    }

    @Test
    void shouldCopyNewDockerImageOnUpdate(WireMockRuntimeInfo runtimeInfo) throws Exception {
        // Given
        copyImage("scratch", imageNameInRegistry(destination, "test:old"));
        copyImage("scratch", imageNameInRegistry(source, "test:new"));

        // When
        handleEvent(event(runtimeInfo)
                .withRequestType("Update")
                .withResourceProperties(Map.of(
                        "source", imageNameInRegistry(source, "test:new"),
                        "target", imageNameInRegistry(destination, "test:new")))
                .withOldResourceProperties(Map.of(
                        "source", imageNameInRegistry(source, "test:old"),
                        "target", imageNameInRegistry(destination, "test:old")))
                .build());

        // Then
        assertThat(registryClient(destination, "test").pullManifest("new"))
                .extracting(ManifestAndDigest::getManifest)
                .isInstanceOf(V22ManifestTemplate.class);
        assertThat(registryClient(destination, "test").pullManifest("old"))
                .extracting(ManifestAndDigest::getManifest)
                .isInstanceOf(V22ManifestTemplate.class);
        verify(putRequestedFor(urlEqualTo("/report-response"))
                .withRequestBody(matchingJsonPath("$.Data.digest", matching("sha256:[a-z0-9]+"))
                        .and(matchingJsonPath("$.Status", equalTo("SUCCESS")))));
    }

    @Test
    void shouldDoNothingOnDelete(WireMockRuntimeInfo runtimeInfo) throws Exception {
        // When
        handleEvent(event(runtimeInfo)
                .withRequestType("Delete")
                .withResourceProperties(Map.of(
                        "source", imageNameInRegistry(source, "test"),
                        "target", imageNameInRegistry(destination, "test")))
                .build());

        // Then
        verify(putRequestedFor(urlEqualTo("/report-response"))
                .withRequestBody(equalToJson("{\"Status\":\"SUCCESS\",\"Data\":null}", true, true)));
    }

    private static void copyImage(String baseImage, String target) throws Exception {
        Jib.from(baseImage).containerize(JibEvents.logEvents(LOGGER, Containerizer.to(RegistryImage.named(target)))
                .setAllowInsecureRegistries(true));
    }

    private static String imageNameInRegistry(GenericContainer<?> container, String name) {
        return container.getHost() + ":" + container.getFirstMappedPort() + "/" + name;
    }

    private static GenericContainer<?> createDockerRegistryContainer() {
        return new GenericContainer<>(DockerImageName.parse("registry"))
                .withExposedPorts(5000)
                .withLogConsumer(outputFrame -> LOGGER.info("From Docker registry: {}", outputFrame.getUtf8StringWithoutLineEnding()));
    }

    private static RegistryClient registryClient(GenericContainer<?> container, String name) {
        EventHandlers eventHandlers = JibEvents.createEventHandlers(LOGGER);
        String serverUrl = container.getHost() + ":" + container.getFirstMappedPort();
        FailoverHttpClient httpClient = new FailoverHttpClient(true, true, eventHandlers::dispatch);
        return RegistryClient.factory(eventHandlers, serverUrl, name, httpClient).newRegistryClient();
    }

    private void handleEvent(CloudFormationCustomResourceEvent event) throws Exception {
        lambda().handleRequest(event, new FakeLambdaContext());
    }

    private CloudFormationCustomResourceEventBuilder event(WireMockRuntimeInfo runtimeInfo) {
        return CloudFormationCustomResourceEvent.builder()
                .withResponseUrl(runtimeInfo.getHttpBaseUrl() + "/report-response");
    }

    private CopyContainerImageLambda lambda() {
        return CopyContainerImageLambda.builder()
                .allowInsecureRegistries(true)
                .build();
    }

}
