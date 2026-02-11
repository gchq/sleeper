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
package sleeper.cdk.artefacts;

import sleeper.core.deploy.DockerDeployment;

/**
 * Code to refer to a Docker image for use when running with a state machine. This is used for bulk import on EKS.
 */
public interface SleeperDockerImageNames {

    /**
     * Retrieves the Docker image name for this deployment. Includes the repository URL and the tag.
     *
     * @param  deployment which deployment we want the Docker image for
     * @return            the Docker image name
     */
    String imageName(DockerDeployment deployment);

}
