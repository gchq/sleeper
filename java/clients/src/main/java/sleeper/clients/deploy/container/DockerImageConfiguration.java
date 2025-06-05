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

import sleeper.clients.admin.properties.PropertiesDiff;
import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.LambdaDeployType;
import sleeper.core.properties.model.OptionalStack;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static sleeper.core.properties.instance.CommonProperty.LAMBDA_DEPLOY_TYPE;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;

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
     * Finds which images need to be uploaded when redeploying due to a change to an instance property. This is computed
     * based on which optional stacks have been added to the instance. This is needed in a context where Docker images
     * are only uploaded to AWS when we deploy the stack associated with it.
     *
     * @param  properties the instance properties
     * @param  diff       the diff describing the change that caused the redeploy
     * @return            the list of Docker images that need to be uploaded
     */
    public List<StackDockerImage> getImagesToUploadOnUpdate(InstanceProperties properties, PropertiesDiff diff) {
        Set<OptionalStack> stacksBefore = diff.getValuesBefore(properties)
                .streamEnumList(OPTIONAL_STACKS, OptionalStack.class)
                .collect(toUnmodifiableSet());
        Set<OptionalStack> stacksAdded = properties.streamEnumList(OPTIONAL_STACKS, OptionalStack.class)
                .filter(not(stacksBefore::contains))
                .collect(toUnmodifiableSet());
        LambdaDeployType lambdaDeployType = properties.getEnumValue(LAMBDA_DEPLOY_TYPE, LambdaDeployType.class);
        return getImagesToUpload(stacksAdded, lambdaDeployType, lambda -> lambda.isDeployedOptional(stacksAdded));
    }

    /**
     * Finds which images need to be uploaded when deploying an instance of Sleeper. This is computed based on which
     * optional stacks will be deployed for the instance.
     *
     * @param  properties the instance properties
     * @return            the list of Docker images that need to be uploaded
     */
    public List<StackDockerImage> getImagesToUpload(InstanceProperties properties) {
        Set<OptionalStack> optionalStacks = properties.streamEnumList(OPTIONAL_STACKS, OptionalStack.class).collect(toUnmodifiableSet());
        LambdaDeployType lambdaDeployType = properties.getEnumValue(LAMBDA_DEPLOY_TYPE, LambdaDeployType.class);
        return getImagesToUpload(optionalStacks, lambdaDeployType, lambda -> lambda.isDeployed(optionalStacks));
    }

    private List<StackDockerImage> getImagesToUpload(
            Collection<OptionalStack> stacks, LambdaDeployType lambdaDeployType, Predicate<LambdaHandler> checkUploadLambda) {
        return Stream.concat(
                dockerDeploymentImages(stacks),
                lambdaImages(lambdaDeployType, checkUploadLambda))
                .collect(toUnmodifiableList());
    }

    private Stream<StackDockerImage> dockerDeploymentImages(Collection<OptionalStack> stacks) {
        return dockerDeployments.stream()
                .filter(deployment -> stacks.contains(deployment.getOptionalStack()))
                .map(StackDockerImage::fromDockerDeployment);
    }

    private Stream<StackDockerImage> lambdaImages(LambdaDeployType lambdaDeployType, Predicate<LambdaHandler> checkUploadLambda) {
        if (lambdaDeployType != LambdaDeployType.CONTAINER) {
            return lambdaHandlers.stream().filter(checkUploadLambda)
                    .map(LambdaHandler::getJar).filter(LambdaJar::isAlwaysDockerDeploy).distinct()
                    .map(StackDockerImage::lambdaImage);
        }
        return lambdaHandlers.stream().filter(checkUploadLambda)
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
}
