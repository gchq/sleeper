/*
 * Copyright 2022-2024 Crown Copyright
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

import sleeper.clients.admin.properties.PropertiesDiff;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.validation.LambdaDeployType;
import sleeper.core.properties.validation.OptionalStack;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static sleeper.clients.deploy.StackDockerImage.dockerBuildImage;
import static sleeper.clients.deploy.StackDockerImage.dockerBuildxImage;
import static sleeper.clients.deploy.StackDockerImage.emrServerlessImage;
import static sleeper.core.properties.instance.CommonProperty.LAMBDA_DEPLOY_TYPE;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;

public class DockerImageConfiguration {
    private static final Map<OptionalStack, List<StackDockerImage>> DEFAULT_DOCKER_IMAGE_BY_STACK = Map.of(
            OptionalStack.IngestStack, List.of(dockerBuildImage("ingest")),
            OptionalStack.EksBulkImportStack, List.of(dockerBuildImage("bulk-import-runner")),
            OptionalStack.CompactionStack, List.of(dockerBuildxImage("compaction-job-execution"), dockerBuildImage("compaction-gpu", "runner")),
            OptionalStack.EmrServerlessBulkImportStack, List.of(emrServerlessImage("bulk-import-runner-emr-serverless")));

    private static final DockerImageConfiguration DEFAULT = new DockerImageConfiguration(DEFAULT_DOCKER_IMAGE_BY_STACK, LambdaHandler.all());

    private final Map<OptionalStack, List<StackDockerImage>> imageByStack;
    private final List<LambdaHandler> lambdaHandlers;

    public static DockerImageConfiguration getDefault() {
        return DEFAULT;
    }

    public DockerImageConfiguration(Map<OptionalStack, List<StackDockerImage>> imageByStack, List<LambdaHandler> lambdaHandlers) {
        this.imageByStack = imageByStack;
        this.lambdaHandlers = lambdaHandlers;
    }

    public List<StackDockerImage> getImagesToUploadOnUpdate(InstanceProperties properties, PropertiesDiff diff) {
        Set<OptionalStack> stacksBefore = diff.getValuesBefore(properties)
                .streamEnumList(OPTIONAL_STACKS, OptionalStack.class)
                .collect(toUnmodifiableSet());
        List<OptionalStack> stacksAdded = properties.streamEnumList(OPTIONAL_STACKS, OptionalStack.class)
                .filter(not(stacksBefore::contains))
                .collect(toUnmodifiableList());
        LambdaDeployType lambdaDeployType = properties.getEnumValue(LAMBDA_DEPLOY_TYPE, LambdaDeployType.class);
        return getImagesToUpload(stacksAdded, lambdaDeployType, lambda -> lambda.isDeployedOptional(stacksAdded));
    }

    public List<StackDockerImage> getImagesToUpload(InstanceProperties properties) {
        List<OptionalStack> optionalStacks = properties.getEnumList(OPTIONAL_STACKS, OptionalStack.class);
        LambdaDeployType lambdaDeployType = properties.getEnumValue(LAMBDA_DEPLOY_TYPE, LambdaDeployType.class);
        return getImagesToUpload(optionalStacks, lambdaDeployType, lambda -> lambda.isDeployed(optionalStacks));
    }

    private List<StackDockerImage> getImagesToUpload(
            Collection<OptionalStack> stacks, LambdaDeployType lambdaDeployType, Predicate<LambdaHandler> checkUploadLambda) {
        return Stream.concat(
                stacks.stream()
                        .map(this::getStackImage)
                        .flatMap(Optional::stream),
                lambdaImages(lambdaDeployType, checkUploadLambda))
                .collect(toUnmodifiableList());
    }

    private Stream<StackDockerImage> lambdaImages(LambdaDeployType lambdaDeployType, Predicate<LambdaHandler> checkUploadLambda) {
        if (lambdaDeployType != LambdaDeployType.CONTAINER) {
            return Stream.empty();
        }
        return lambdaHandlers.stream().filter(checkUploadLambda)
                .map(LambdaHandler::getJar).distinct()
                .map(StackDockerImage::lambdaImage);
    }

    private Optional<List<StackDockerImage>> getStackImage(OptionalStack stack) {
        return Optional.ofNullable(imageByStack.get(stack));
    }

    public Optional<String> getInstanceIdFromRepoName(String repositoryName) {
        return imageByStack.values().stream()
                .filter(image -> repositoryName.endsWith("/" + image.get(0).getImageName()))
                .map(image -> repositoryName.substring(0, repositoryName.indexOf("/")))
                .findFirst();
    }
}
