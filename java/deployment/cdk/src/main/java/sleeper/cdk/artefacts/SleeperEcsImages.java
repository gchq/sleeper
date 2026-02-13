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

import software.amazon.awscdk.services.ecs.ContainerImage;

import sleeper.core.deploy.DockerDeployment;

/**
 * Code to refer to a Docker image for use when deploying to ECS.
 */
public interface SleeperEcsImages {

    /**
     * Retrieves a reference to a Docker image. The scope and ID can be used in case it is necessary to create a new
     * construct to refer to the image.
     *
     * @param  deployment which deployment we want the Docker image for
     * @return            the reference to the Docker image
     */
    ContainerImage containerImage(DockerDeployment deployment);

}
