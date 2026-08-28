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

import sleeper.clients.util.command.CommandPipelineRunner;

import java.io.IOException;

/**
 * A destination for base images to be uploaded for use when building other images. This should fulfil the needs of an
 * image builder to access the base images.
 */
public interface BaseImageDestination {

    /**
     * Returns a destination that pushes base images to the same registry as other images. This will only work if the
     * registry does not require repositories to be created explicitly.
     *
     * @return the destination
     */
    static BaseImageDestination deploymentRegistry() {
        return new DeploymentRegistry();
    }

    /**
     * Returns a destination that creates and manages a Docker container for a local registry to push base images to.
     *
     * @param  port the port to expose the registry on
     * @return      the destination
     */
    static BaseImageDestination managedRegistry(int port) {
        return new ManagedBaseImageRegistry(port);
    }

    /**
     * Returns a destination that pushes base images to a pre-existing registry.
     *
     * @param  baseImagePrefix the prefix for base image names
     * @return                 the destination
     */
    static BaseImageDestination fixedRegistry(String baseImagePrefix) {
        return new FixedBaseImageRegistry(baseImagePrefix);
    }

    /**
     * Brings the destination into a state where base images can be pushed to it and pulled from it.
     *
     * @param  commandRunner        a runner to interact with the command line
     * @throws IOException          if a command could not be run
     * @throws InterruptedException if the thread was interrupted while running a command
     */
    void createIfMissing(CommandPipelineRunner commandRunner) throws IOException, InterruptedException;

    /**
     * Returns the repository prefix that base images are tagged with, and that builds resolve them from.
     *
     * @param  deploymentRepositoryPrefix the prefix for other images
     * @return                            the prefix for base images
     */
    String repositoryPrefix(String deploymentRepositoryPrefix);
}
