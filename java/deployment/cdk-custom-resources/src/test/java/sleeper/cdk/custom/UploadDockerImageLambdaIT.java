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
import com.google.cloud.tools.jib.api.RegistryImage;
import com.google.cloud.tools.jib.event.EventHandlers;
import com.google.cloud.tools.jib.http.FailoverHttpClient;
import com.google.cloud.tools.jib.image.json.V22ManifestTemplate;
import com.google.cloud.tools.jib.registry.ManifestAndDigest;
import com.google.cloud.tools.jib.registry.RegistryClient;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.cdk.custom.containers.JibEvents;

import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class UploadDockerImageLambdaIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadDockerImageLambdaIT.class);

    @Container
    public GenericContainer<?> source = createDockerRegistryContainer();

    @Container
    public GenericContainer<?> destination = createDockerRegistryContainer();

    // TODO
    // - Output uploaded image digest
    // - Authentication for ECR

    @Test
    void shouldCopyDockerImageOnCreate() throws Exception {
        // Given
        copyImage("busybox", imageNameInRegistry(source, "test"));

        // When
        Map<String, Object> output = handleEvent(CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(Map.of(
                        "source", imageNameInRegistry(source, "test"),
                        "target", imageNameInRegistry(destination, "test")))
                .build());

        // Then
        assertThat(output)
                .extractingByKey("Data", InstanceOfAssertFactories.MAP)
                .extractingByKey("digest", InstanceOfAssertFactories.STRING)
                .matches(Pattern.compile("sha256:[a-z0-9]+"));
        assertThat(registryClient(destination, "test").pullManifest("latest"))
                .extracting(ManifestAndDigest::getManifest)
                .isInstanceOf(V22ManifestTemplate.class);
    }

    @Test
    void shouldCopyNewDockerImageOnUpdate() throws Exception {
        // Given
        copyImage("busybox", imageNameInRegistry(destination, "test:old"));
        copyImage("busybox", imageNameInRegistry(source, "test:new"));

        // When
        Map<String, Object> output = handleEvent(CloudFormationCustomResourceEvent.builder()
                .withRequestType("Update")
                .withResourceProperties(Map.of(
                        "source", imageNameInRegistry(source, "test:new"),
                        "target", imageNameInRegistry(destination, "test:new")))
                .withOldResourceProperties(Map.of(
                        "source", imageNameInRegistry(source, "test:old"),
                        "target", imageNameInRegistry(destination, "test:old")))
                .build());

        // Then
        assertThat(output)
                .extractingByKey("Data", InstanceOfAssertFactories.MAP)
                .extractingByKey("digest", InstanceOfAssertFactories.STRING)
                .matches(Pattern.compile("sha256:[a-z0-9]+"));
        assertThat(registryClient(destination, "test").pullManifest("new"))
                .extracting(ManifestAndDigest::getManifest)
                .isInstanceOf(V22ManifestTemplate.class);
        assertThat(registryClient(destination, "test").pullManifest("old"))
                .extracting(ManifestAndDigest::getManifest)
                .isInstanceOf(V22ManifestTemplate.class);
    }

    @Test
    void shouldDoNothingOnDelete() throws Exception {
        // Given
        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Delete")
                .withResourceProperties(Map.of(
                        "source", imageNameInRegistry(source, "test"),
                        "target", imageNameInRegistry(destination, "test")))
                .build();

        // When
        Map<String, Object> output = handleEvent(event);

        // Then
        assertThat(output).isEmpty();
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

    private Map<String, Object> handleEvent(CloudFormationCustomResourceEvent event) throws Exception {
        return lambda().handleEvent(event, null);
    }

    private UploadDockerImageLambda lambda() {
        return UploadDockerImageLambda.allowInsecureRegistries();
    }

}
