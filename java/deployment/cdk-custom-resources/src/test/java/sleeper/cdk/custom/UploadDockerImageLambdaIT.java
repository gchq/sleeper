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
import com.google.cloud.tools.jib.api.Containerizer;
import com.google.cloud.tools.jib.api.Jib;
import com.google.cloud.tools.jib.api.LogEvent;
import com.google.cloud.tools.jib.api.RegistryImage;
import com.google.cloud.tools.jib.event.EventHandlers;
import com.google.cloud.tools.jib.http.FailoverHttpClient;
import com.google.cloud.tools.jib.image.json.V22ManifestTemplate;
import com.google.cloud.tools.jib.registry.ManifestAndDigest;
import com.google.cloud.tools.jib.registry.RegistryClient;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@Testcontainers
public class UploadDockerImageLambdaIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadDockerImageLambdaIT.class);

    @Container
    public GenericContainer<?> source = createDockerRegistryContainer();

    @Container
    public GenericContainer<?> destination = createDockerRegistryContainer();

    @Test
    void shouldCopyDockerImageOnCreate() throws Exception {
        // Given
        copyImage("busybox", imageNameInRegistry(source, "test"));

        // When
        handleEvent(CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(Map.of(
                        "source", imageNameInRegistry(source, "test"),
                        "target", imageNameInRegistry(destination, "test")))
                .build());

        // Then
        assertThat(registryClient(destination, "test").pullManifest("latest"))
                .extracting(ManifestAndDigest::getManifest)
                .isInstanceOf(V22ManifestTemplate.class);
    }

    @Test
    void shouldCopyNewDockerImageOnUpdate() {
        // TODO
    }

    @Test
    void shouldDoNothingOnDelete() {
        // Given
        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Delete")
                .withResourceProperties(Map.of(
                        "source", imageNameInRegistry(source, "test"),
                        "target", imageNameInRegistry(destination, "test")))
                .build();

        // When / Then
        assertThatCode(() -> handleEvent(event))
                .doesNotThrowAnyException();
    }

    private static void copyImage(String baseImage, String target) throws Exception {
        Jib.from(baseImage).containerize(Containerizer.to(RegistryImage.named(target))
                .addEventHandler(LogEvent.class, event -> LOGGER.info("From Jib: {}", event))
                .setAllowInsecureRegistries(true));
    }

    private static String imageNameInRegistry(GenericContainer<?> container, String name) {
        return container.getHost() + ":" + container.getFirstMappedPort() + "/" + name;
    }

    private static GenericContainer<?> createDockerRegistryContainer() {
        return new GenericContainer<>(DockerImageName.parse("registry"))
                .withExposedPorts(5000)
                .withLogConsumer(outputFrame -> LOGGER.info("From registry: {}", outputFrame.getUtf8StringWithoutLineEnding()));
    }

    private static RegistryClient registryClient(GenericContainer<?> container, String name) {
        EventHandlers eventHandlers = EventHandlers.builder()
                .add(LogEvent.class, event -> LOGGER.info("From Jib: {}", event))
                .build();
        String serverUrl = container.getHost() + ":" + container.getFirstMappedPort();
        FailoverHttpClient httpClient = new FailoverHttpClient(true, true, eventHandlers::dispatch);
        return RegistryClient.factory(eventHandlers, serverUrl, name, httpClient).newRegistryClient();
    }

    private void handleEvent(CloudFormationCustomResourceEvent event) throws Exception {
        lambda().handleEvent(event, null);
    }

    private UploadDockerImageLambda lambda() {
        return UploadDockerImageLambda.allowInsecureRegistries();
    }

}
