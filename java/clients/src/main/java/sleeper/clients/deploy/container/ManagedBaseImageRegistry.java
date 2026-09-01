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
package sleeper.clients.deploy.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.util.command.CommandPipeline;
import sleeper.clients.util.command.CommandPipelineRunner;

import java.io.IOException;
import java.util.Objects;

import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;

/**
 * A base image destination that manages a local registry as a long-lived Docker container.
 */
class ManagedBaseImageRegistry implements BaseImageDestination {
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedBaseImageRegistry.class);

    private static final String CONTAINER_NAME = "sleeper-base-image-registry";
    private static final String VOLUME_NAME = "sleeper-base-image-registry";
    private static final String REGISTRY_IMAGE = "registry:2";
    private static final int PORT_IN_CONTAINER = 5000;

    private final int port;

    ManagedBaseImageRegistry(int port) {
        this.port = port;
    }

    @Override
    public void createIfMissing(CommandPipelineRunner commandRunner) throws IOException, InterruptedException {
        LOGGER.info("Reconciling base image registry container {} on port {}", CONTAINER_NAME, port);
        if (commandRunner.run(startContainer()).getLastExitCode() == 0) {
            return;
        }
        int exitCode = commandRunner.run(createContainer()).getLastExitCode();
        // Creating it can clash with a deploy running alongside this one that found it absent at the same moment, so
        // starting it once more distinguishes losing that race from the registry being genuinely unavailable.
        if (exitCode != 0) {
            commandRunner.runOrThrow(startContainer());
        }
    }

    @Override
    public String repositoryPrefix(String deploymentRepositoryPrefix) {
        return "localhost:" + port;
    }

    @Override
    public String toString() {
        return "managed base image registry at localhost:" + port;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        return ((ManagedBaseImageRegistry) obj).port == port;
    }

    @Override
    public int hashCode() {
        return Objects.hash(port);
    }

    private static CommandPipeline startContainer() {
        return pipeline(command("docker", "start", CONTAINER_NAME));
    }

    private CommandPipeline createContainer() {
        return pipeline(command("docker", "run", "-d",
                "--name", CONTAINER_NAME,
                "-p", "127.0.0.1:" + port + ":" + PORT_IN_CONTAINER,
                "-v", VOLUME_NAME + ":/var/lib/registry",
                REGISTRY_IMAGE));
    }
}
