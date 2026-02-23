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
package sleeper.cdk.artefacts.containers;

import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.lambda.DockerImageCode;
import software.amazon.awscdk.services.lambda.EcrImageCodeProps;
import software.constructs.Construct;

import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;

public class SleeperContainerImagesFromProperties implements SleeperContainerImages {

    private final InstanceProperties instanceProperties;

    public SleeperContainerImagesFromProperties(InstanceProperties instanceProperties) {
        this.instanceProperties = instanceProperties;
    }

    @Override
    public SleeperEcsImages ecsImagesAtScope(Construct scope) {
        SleeperEcrRepositoriesAtScope repositories = new SleeperEcrRepositoriesAtScope(scope, instanceProperties);
        return deployment -> ContainerImage.fromEcrRepository(
                repositories.getRepository(deployment),
                instanceProperties.get(VERSION));
    }

    @Override
    public SleeperLambdaImages lambdaImagesAtScope(Construct scope) {
        SleeperEcrRepositoriesAtScope repositories = new SleeperEcrRepositoriesAtScope(scope, instanceProperties);
        return handler -> DockerImageCode.fromEcr(
                repositories.getRepository(handler.getJar()),
                EcrImageCodeProps.builder()
                        .cmd(List.of(handler.getHandler()))
                        .tagOrDigest(instanceProperties.get(VERSION))
                        .build());
    }

}
