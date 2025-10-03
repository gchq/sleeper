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
package sleeper.clients.deploy;

import sleeper.clients.deploy.container.DockerImageLocation;

import java.util.Objects;

public record DeployConfiguration(DockerImageLocation dockerImageLocation, String dockerRepositoryPrefix) {

    public DeployConfiguration {
        Objects.requireNonNull(dockerImageLocation, "dockerImageLocation must not be null");
        if (dockerImageLocation == DockerImageLocation.REPOSITORY) {
            Objects.requireNonNull(dockerRepositoryPrefix, "dockerRepositoryPrefix must not be null");
        }
    }

    public static DeployConfiguration fromLocalBuild() {
        return new DeployConfiguration(DockerImageLocation.LOCAL_BUILD, null);
    }

    public static DeployConfiguration fromDockerRepository(String prefix) {
        return new DeployConfiguration(DockerImageLocation.REPOSITORY, prefix);
    }
}
