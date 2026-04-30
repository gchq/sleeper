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
package sleeper.container.images;

import com.google.cloud.tools.jib.api.Containerizer;
import com.google.cloud.tools.jib.api.Jib;
import com.google.cloud.tools.jib.api.RegistryImage;
import com.google.cloud.tools.jib.event.EventHandlers;
import com.google.cloud.tools.jib.http.FailoverHttpClient;
import com.google.cloud.tools.jib.image.json.ManifestTemplate;
import com.google.cloud.tools.jib.image.json.V22ManifestTemplate;
import com.google.cloud.tools.jib.registry.ManifestAndDigest;
import com.google.cloud.tools.jib.registry.RegistryClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
public class ContainerImageTransferManagerIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(ContainerImageTransferManagerIT.class);

    @Container
    public static final GenericContainer<?> SOURCE = createDockerRegistryContainer();
    @Container
    public static final GenericContainer<?> DESTINATION = createDockerRegistryContainer();
    @TempDir
    private Path cacheDir;
    private final String imageName = UUID.randomUUID().toString();

    @Test
    void shouldCopyDockerImage() throws Exception {
        // Given
        copyImage("scratch", imageNameInRegistry(SOURCE));

        // When
        ContainerImageTransferResult result = transferManager().transfer(ContainerImageTransferRequest.builder()
                .sourceImageReference(imageNameInRegistry(SOURCE))
                .targetImageReference(imageNameInRegistry(DESTINATION))
                .build());

        // Then
        ManifestAndDigest<ManifestTemplate> manifest = imageClientForRegistry(DESTINATION).pullManifest("latest");
        assertThat(manifest.getManifest()).isInstanceOf(V22ManifestTemplate.class);
        assertThat(manifest.getDigest()).hasToString(result.digest());
        assertThat(cacheDir).isNotEmptyDirectory();
        assertThat(result.digest()).matches("sha256:[a-z0-9]+");
    }

    @Test
    void shouldCopyDockerImageByTag() throws Exception {
        // Given
        copyImage("scratch", imageNameInRegistryWithTag(SOURCE, "source-tag"));

        // When
        ContainerImageTransferResult result = transferManager().transfer(ContainerImageTransferRequest.builder()
                .sourceImageReference(imageNameInRegistryWithTag(SOURCE, "source-tag"))
                .targetImageReference(imageNameInRegistryWithTag(DESTINATION, "target-tag"))
                .build());

        // Then
        ManifestAndDigest<ManifestTemplate> manifest = imageClientForRegistry(DESTINATION).pullManifest("target-tag");
        assertThat(manifest.getManifest()).isInstanceOf(V22ManifestTemplate.class);
        assertThat(manifest.getDigest()).hasToString(result.digest());
        assertThat(cacheDir).isNotEmptyDirectory();
        assertThat(result.digest()).matches("sha256:[a-z0-9]+");
    }

    @Test
    void shouldFailWhenSourceTagDoesNotExist() throws Exception {
        // Given
        copyImage("scratch", imageNameInRegistry(SOURCE));

        // When / Then
        assertThatThrownBy(() -> transferManager()
                .transfer(ContainerImageTransferRequest.builder()
                        .sourceImageReference(imageNameInRegistryWithTag(SOURCE, "non-existent"))
                        .targetImageReference(imageNameInRegistry(DESTINATION))
                        .build()))
                .isInstanceOf(ContainerImageTransferException.class);
    }

    private ContainerImageTransferManager transferManager() {
        return ContainerImageTransferManager.builder()
                .allowInsecureRegistries(true)
                .cacheDir(cacheDir)
                .build();
    }

    private static void copyImage(String baseImage, String target) throws Exception {
        Jib.from(baseImage).containerize(JibEvents.logEvents(LOGGER, Containerizer.to(RegistryImage.named(target)))
                .setAllowInsecureRegistries(true));
    }

    private String imageNameInRegistry(GenericContainer<?> container) {
        return container.getHost() + ":" + container.getFirstMappedPort() + "/" + imageName;
    }

    private String imageNameInRegistryWithTag(GenericContainer<?> container, String tag) {
        return container.getHost() + ":" + container.getFirstMappedPort() + "/" + imageName + ":" + tag;
    }

    private static GenericContainer<?> createDockerRegistryContainer() {
        return new GenericContainer<>(DockerImageName.parse("registry"))
                .withExposedPorts(5000)
                .withLogConsumer(outputFrame -> LOGGER.info("From Docker registry: {}", outputFrame.getUtf8StringWithoutLineEnding()));
    }

    private RegistryClient imageClientForRegistry(GenericContainer<?> container) {
        EventHandlers eventHandlers = JibEvents.createEventHandlers(LOGGER);
        String serverUrl = container.getHost() + ":" + container.getFirstMappedPort();
        FailoverHttpClient httpClient = new FailoverHttpClient(true, true, eventHandlers::dispatch);
        return RegistryClient.factory(eventHandlers, serverUrl, imageName, httpClient).newRegistryClient();
    }
}
