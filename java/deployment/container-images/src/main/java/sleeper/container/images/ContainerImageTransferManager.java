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

import com.google.cloud.tools.jib.api.CacheDirectoryCreationException;
import com.google.cloud.tools.jib.api.Containerizer;
import com.google.cloud.tools.jib.api.Credential;
import com.google.cloud.tools.jib.api.ImageReference;
import com.google.cloud.tools.jib.api.InvalidImageReferenceException;
import com.google.cloud.tools.jib.api.Jib;
import com.google.cloud.tools.jib.api.JibContainer;
import com.google.cloud.tools.jib.api.JibContainerBuilder;
import com.google.cloud.tools.jib.api.RegistryException;
import com.google.cloud.tools.jib.api.RegistryImage;
import com.google.cloud.tools.jib.api.buildplan.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.deploy.ContainerPlatform;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Transfers a container image from one repository to another. This supports multiplatform images. At the time of
 * writing the Docker CLI `docker push` does not work for multiplatform images.
 */
public class ContainerImageTransferManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ContainerImageTransferManager.class);

    private final Path cacheDir;
    private final boolean allowInsecureRegistries;

    private ContainerImageTransferManager(Builder builder) {
        cacheDir = builder.cacheDir;
        allowInsecureRegistries = builder.allowInsecureRegistries;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Performs a container image transfer.
     *
     * @param  request the request
     * @return         the result
     */
    public ContainerImageTransferResult transfer(ContainerImageTransferRequest request) throws InterruptedException {
        try {
            ImageReference sourceRef = ImageReference.parse(request.getSourceImageReference());
            ImageReference targetRef = ImageReference.parse(request.getTargetImageReference());
            JibContainerBuilder containerBuilder = Jib.from(registryImage(sourceRef, request.getSourceCredentialsRetriever()));
            setPlatformsIfPresent(containerBuilder, request.getPlatforms());
            JibContainer container = containerBuilder
                    .containerize(configure(Containerizer.to(registryImage(targetRef, request.getTargetCredentialsRetriever()))));
            return new ContainerImageTransferResult(
                    container.getTargetImage().toString(),
                    container.getDigest().toString());
        } catch (IOException | RegistryException | CacheDirectoryCreationException | ExecutionException | InvalidImageReferenceException e) {
            throw new ContainerImageTransferException(request, e);
        }
    }

    private static void setPlatformsIfPresent(JibContainerBuilder containerBuilder, List<ContainerPlatform> platforms) {
        if (platforms.isEmpty()) {
            return;
        }
        Set<Platform> jibPlatforms = new LinkedHashSet<>();
        for (ContainerPlatform platform : platforms) {
            jibPlatforms.add(new Platform(platform.architecture(), platform.os()));
        }
        containerBuilder.setPlatforms(jibPlatforms);
    }

    private static RegistryImage registryImage(ImageReference imageName, ContainerRegistryCredentials.Retriever credentialRetriever) throws InvalidImageReferenceException {
        RegistryImage image = RegistryImage.named(imageName);
        if (credentialRetriever != null) {
            image.addCredentialRetriever(() -> credentialRetriever.retrieve()
                    .map(credentials -> Credential.from(credentials.username(), credentials.password())));
        }
        return image;
    }

    private Containerizer configure(Containerizer containerizer) {
        containerizer.setBaseImageLayersCache(cacheDir.resolve("base"));
        containerizer.setApplicationLayersCache(cacheDir.resolve("app"));
        containerizer.setAllowInsecureRegistries(allowInsecureRegistries);
        return JibEvents.logEvents(LOGGER, containerizer);
    }

    /**
     * A builder to create a container image transfer manager.
     */
    public static class Builder {
        private Path cacheDir;
        private boolean allowInsecureRegistries;

        /**
         * Sets the directory to store the Jib cache. Defaults to a location in the home directory if not set.
         *
         * @param  cacheDir the cache directory
         * @return          this builder
         */
        public Builder cacheDir(Path cacheDir) {
            this.cacheDir = cacheDir;
            return this;
        }

        /**
         * Sets whether to allow interacting with insecure registries, without encryption. Usually this should only be
         * used for testing.
         *
         * @param  allowInsecureRegistries true if insecure registries should be allowed
         * @return                         this builder
         */
        public Builder allowInsecureRegistries(boolean allowInsecureRegistries) {
            this.allowInsecureRegistries = allowInsecureRegistries;
            return this;
        }

        public ContainerImageTransferManager build() {
            return new ContainerImageTransferManager(this);
        }
    }

}
