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

package sleeper.clients.deploy.container;

import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.SleeperPropertyValues;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.model.LambdaDeployType;
import sleeper.core.properties.model.OptionalStack;
import sleeper.core.properties.model.StateStoreCommitterPlatform;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static sleeper.core.properties.instance.CommonProperty.LAMBDA_DEPLOY_TYPE;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_PLATFORM;

/**
 * Models which Docker images need to be built and uploaded to be able to deploy the system. This includes components
 * that are always deployed based on a Docker image, e.g. ECS tasks, as well as lambdas that may or may not be deployed
 * with Docker. This combines these different types of components to determine which Docker images need to be present
 * during a certain deployment.
 */
public class DockerImageConfiguration {

    private static final DockerImageConfiguration DEFAULT = new DockerImageConfiguration(DockerDeployment.all(), LambdaHandler.all());

    private final List<DockerDeployment> dockerDeployments;
    private final List<LambdaHandler> lambdaHandlers;

    public static DockerImageConfiguration getDefault() {
        return DEFAULT;
    }

    public DockerImageConfiguration(List<DockerDeployment> dockerDeployments, List<LambdaHandler> lambdaHandlers) {
        this.dockerDeployments = dockerDeployments;
        this.lambdaHandlers = lambdaHandlers;
    }

    /**
     * Finds which images need to be uploaded when deploying an instance of Sleeper. This is computed based on which
     * optional stacks will be deployed for the instance.
     *
     * @param  properties the instance properties
     * @return            the list of Docker images that need to be uploaded
     */
    public List<StackDockerImage> getImagesToUpload(SleeperPropertyValues<InstanceProperty> properties) {
        return Stream.concat(
                dockerDeploymentImages(properties),
                lambdaImages(properties))
                .collect(toUnmodifiableList());
    }

    public List<StackDockerImage> getAllImagesToUpload() {
        return Stream.concat(
                dockerDeployments.stream()
                        .map(StackDockerImage::fromDockerDeployment),
                lambdaHandlers.stream()
                        .map(LambdaHandler::getJar).distinct()
                        .map(StackDockerImage::lambdaImage))
                .collect(toUnmodifiableList());
    }

    private Stream<StackDockerImage> dockerDeploymentImages(SleeperPropertyValues<InstanceProperty> properties) {
        StateStoreCommitterPlatform committerPlatform = properties.getEnumValue(STATESTORE_COMMITTER_PLATFORM, StateStoreCommitterPlatform.class);
        Set<OptionalStack> stacks = properties.streamEnumList(OPTIONAL_STACKS, OptionalStack.class).collect(toUnmodifiableSet());
        return dockerDeployments.stream()
                .filter(deployment -> deployment.isDeployed(committerPlatform, stacks))
                .map(StackDockerImage::fromDockerDeployment);
    }

    private Stream<StackDockerImage> lambdaImages(SleeperPropertyValues<InstanceProperty> properties) {
        LambdaDeployType lambdaDeployType = properties.getEnumValue(LAMBDA_DEPLOY_TYPE, LambdaDeployType.class);
        Set<OptionalStack> stacks = properties.streamEnumList(OPTIONAL_STACKS, OptionalStack.class).collect(toUnmodifiableSet());
        return lambdaHandlers.stream()
                .filter(lambda -> lambda.isDeployed(lambdaDeployType, stacks))
                .map(LambdaHandler::getJar).distinct()
                .map(StackDockerImage::lambdaImage);
    }

    /**
     * Finds the prefix of an ECR repository that may be a Sleeper instance ID. This assumes that the repository was
     * created during the deployment of a Sleeper instance with no ECR repository prefix set.
     *
     * @param  repositoryName the ECR repository name
     * @return                the detected Sleeper instance ID, if one was found
     */
    public Optional<String> getInstanceIdFromRepoName(String repositoryName) {
        return dockerDeployments.stream()
                .filter(image -> repositoryName.endsWith("/" + image.getDeploymentName()))
                .map(image -> repositoryName.substring(0, repositoryName.indexOf("/")))
                .findFirst();
    }

    public Optional<LambdaJar> getLambdaJarByImageName(String imageName) {
        return lambdaHandlers.stream()
                .map(LambdaHandler::getJar)
                .filter(jar -> Objects.equals(jar.getImageName(), imageName))
                .findFirst();
    }

    public Optional<DockerDeployment> getDockerDeploymentByName(String name) {
        return dockerDeployments.stream()
                .filter(deployment -> Objects.equals(deployment.getDeploymentName(), name))
                .findFirst();
    }
}
