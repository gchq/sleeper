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

import software.amazon.awscdk.CustomResource;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.lambda.DockerImageCode;
import software.amazon.awscdk.services.lambda.EcrImageCodeProps;
import software.constructs.Construct;

import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SleeperContainerImagesFromStack implements SleeperContainerImages {

    private final InstanceProperties instanceProperties;
    private final CopyContainerImageStack stack;
    private final String sourceRepositoryPrefix;

    public SleeperContainerImagesFromStack(InstanceProperties instanceProperties, CopyContainerImageStack stack, String sourceRepositoryPrefix) {
        this.instanceProperties = instanceProperties;
        this.stack = stack;
        this.sourceRepositoryPrefix = sourceRepositoryPrefix;
    }

    @Override
    public SleeperEcsImages ecsImagesAtScope(Construct scope) {
        SleeperEcrRepositoriesAtScope repositories = new SleeperEcrRepositoriesAtScope(scope, instanceProperties);
        CopyContainerAtScope copyContainer = new CopyContainerAtScope(scope);
        return deployment -> ContainerImage.fromEcrRepository(
                repositories.getRepository(deployment),
                copyContainer.getDigest(deployment));
    }

    @Override
    public SleeperLambdaImages lambdaImagesAtScope(Construct scope) {
        SleeperEcrRepositoriesAtScope repositories = new SleeperEcrRepositoriesAtScope(scope, instanceProperties);
        CopyContainerAtScope copyContainer = new CopyContainerAtScope(scope);
        return handler -> DockerImageCode.fromEcr(
                repositories.getRepository(handler.getJar()),
                EcrImageCodeProps.builder()
                        .cmd(List.of(handler.getHandler()))
                        .tagOrDigest(copyContainer.getDigest(handler.getJar()))
                        .build());
    }

    private class CopyContainerAtScope {
        private final Construct scope;
        private final Map<String, CustomResource> imageNameToCustomResource = new HashMap<>();

        CopyContainerAtScope(Construct scope) {
            this.scope = scope;
        }

        public String getDigest(DockerDeployment deployment) {
            return getDigest(deployment.getDeploymentName(), deployment.getDockerImageName(instanceProperties));
        }

        public String getDigest(LambdaJar jar) {
            return getDigest(jar.getImageName(), jar.getDockerImageName(instanceProperties));
        }

        private String getDigest(String imageName, String target) {
            CustomResource customResource = imageNameToCustomResource.computeIfAbsent(imageName, name -> {
                String source = sourceRepositoryPrefix + "/" + imageName;
                return stack.createCopyContainerImage(scope, "copy-container-" + name, source, target);
            });
            return customResource.getAttString("digest");
        }

    }

}
