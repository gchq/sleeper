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

import com.google.cloud.tools.jib.api.Containerizer;
import com.google.cloud.tools.jib.api.Jib;
import com.google.cloud.tools.jib.api.LogEvent;
import com.google.cloud.tools.jib.api.RegistryImage;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class UploadDockerImageLambdaIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadDockerImageLambdaIT.class);

    @Container
    public GenericContainer<?> source = createDockerRegistryContainer();

    @Container
    public GenericContainer<?> destination = createDockerRegistryContainer();

    @Test
    void shouldPullAndPushDockerImage() throws Exception {
        copyToRegistry("busybox", registry(source, "test"));
    }

    private static void copyToRegistry(String baseImage, RegistryImage target) throws Exception {
        Jib.from(baseImage).containerize(Containerizer.to(target)
                .addEventHandler(LogEvent.class, event -> LOGGER.info("From Jib: {}", event))
                .setAllowInsecureRegistries(true));
    }

    private static RegistryImage registry(GenericContainer<?> container, String name) throws Exception {
        String host = container.getHost();
        int port = container.getFirstMappedPort();
        LOGGER.info("Found container host: {}", host);
        LOGGER.info("Found container port: {}", port);
        return RegistryImage.named(host + ":" + port + "/" + name);
    }

    private static GenericContainer<?> createDockerRegistryContainer() {
        return new GenericContainer<>(DockerImageName.parse("registry"))
                .withExposedPorts(5000)
                .withLogConsumer(outputFrame -> LOGGER.info("From registry: {}", outputFrame.getUtf8StringWithoutLineEnding()));
    }

}
