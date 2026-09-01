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
import java.util.Objects;

/**
 * A base image destination that fixes the repository prefix where base images are held.
 */
class FixedBaseImageRegistry implements BaseImageDestination {

    private final String baseImagePrefix;

    FixedBaseImageRegistry(String baseImagePrefix) {
        this.baseImagePrefix = baseImagePrefix;
    }

    @Override
    public void createIfMissing(CommandPipelineRunner commandRunner) throws IOException, InterruptedException {
        // There's nothing to create as we're pointing to a registry that already exists.
    }

    @Override
    public String repositoryPrefix(String deploymentRepositoryPrefix) {
        return baseImagePrefix;
    }

    @Override
    public String toString() {
        return "base image registry at " + baseImagePrefix;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        return Objects.equals(baseImagePrefix, ((FixedBaseImageRegistry) obj).baseImagePrefix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseImagePrefix);
    }

}
