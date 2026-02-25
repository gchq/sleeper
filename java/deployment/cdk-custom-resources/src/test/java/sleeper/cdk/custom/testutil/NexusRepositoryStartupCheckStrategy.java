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
package sleeper.cdk.custom.testutil;

import com.github.dockerjava.api.DockerClient;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.startupcheck.StartupCheckStrategy;

import java.io.IOException;
import java.io.InputStream;

public class NexusRepositoryStartupCheckStrategy extends StartupCheckStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(NexusRepositoryStartupCheckStrategy.class);

    @Override
    public StartupStatus checkStartupState(DockerClient dockerClient, String containerId) {
        try (
                InputStream inputStream = dockerClient.copyArchiveFromContainerCmd(containerId, "/nexus-data/admin.password").exec();
                TarArchiveInputStream tarInputStream = new TarArchiveInputStream(inputStream)) {
            tarInputStream.getNextEntry();
            return StartupStatus.SUCCESSFUL;
        } catch (IOException e) {
            LOGGER.info("Failure getting admin password", e);
            return StartupStatus.NOT_YET_KNOWN;
        }
    }

}
