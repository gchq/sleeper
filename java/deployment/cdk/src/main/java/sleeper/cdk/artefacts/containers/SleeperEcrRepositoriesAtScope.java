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

import software.amazon.awscdk.services.ecr.IRepository;
import software.amazon.awscdk.services.ecr.Repository;
import software.constructs.Construct;

import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.HashMap;
import java.util.Map;

public class SleeperEcrRepositoriesAtScope {
    private final Construct scope;
    private final InstanceProperties instanceProperties;
    private final Map<String, IRepository> imageNameToRepository = new HashMap<>();

    public SleeperEcrRepositoriesAtScope(Construct scope, InstanceProperties instanceProperties) {
        this.scope = scope;
        this.instanceProperties = instanceProperties;
    }

    public IRepository getRepository(LambdaJar jar) {
        return imageNameToRepository.computeIfAbsent(jar.getImageName(),
                imageName -> createRepositoryReference(jar));
    }

    public IRepository getRepository(DockerDeployment deployment) {
        return imageNameToRepository.computeIfAbsent(deployment.getDeploymentName(),
                imageName -> createRepositoryReference(deployment));
    }

    private IRepository createRepositoryReference(LambdaJar jar) {
        String id = jar.getImageName() + "-repository";
        return Repository.fromRepositoryName(scope, id, jar.getEcrRepositoryName(instanceProperties));
    }

    private IRepository createRepositoryReference(DockerDeployment deployment) {
        String id = deployment.getDeploymentName() + "-repository";
        return Repository.fromRepositoryName(scope, id, deployment.getEcrRepositoryName(instanceProperties));
    }

}
