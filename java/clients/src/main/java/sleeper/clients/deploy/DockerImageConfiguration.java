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
import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.validation.LambdaDeployType;
import sleeper.core.properties.validation.OptionalStack;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
    private static final Map<OptionalStack, StackDockerImage> DEFAULT_DOCKER_IMAGE_BY_STACK = Map.of(
            OptionalStack.IngestStack, dockerBuildImage("ingest"),
            OptionalStack.EksBulkImportStack, dockerBuildImage("bulk-import-runner"),
            OptionalStack.CompactionStack, dockerBuildxImage("compaction-job-execution"),
            OptionalStack.EmrServerlessBulkImportStack, emrServerlessImage("bulk-import-runner-emr-serverless"));

    private static final DockerImageConfiguration DEFAULT = new DockerImageConfiguration(DEFAULT_DOCKER_IMAGE_BY_STACK, LambdaJar.all());

    private final Map<OptionalStack, StackDockerImage> imageByStack;
    private final List<LambdaJar> lambdaJars;

    public static DockerImageConfiguration getDefault() {
        return DEFAULT;
    }

    public DockerImageConfiguration(Map<OptionalStack, StackDockerImage> imageByStack, List<LambdaJar> lambdaJars) {
        this.imageByStack = imageByStack;
        this.lambdaJars = lambdaJars;
    }

    public List<StackDockerImage> getImagesToDeploy(InstanceProperties properties, PropertiesDiff diff) {
        LambdaDeployType lambdaDeployType = properties.getEnumValue(LAMBDA_DEPLOY_TYPE, LambdaDeployType.class);
        Set<OptionalStack> stacksBefore = diff.getValuesBefore(properties)
                .streamEnumList(OPTIONAL_STACKS, OptionalStack.class)
                .collect(toUnmodifiableSet());
        List<OptionalStack> stacksAdded = properties.streamEnumList(OPTIONAL_STACKS, OptionalStack.class)
                .filter(not(stacksBefore::contains))
                .collect(toUnmodifiableList());
        if (stacksAdded.isEmpty() && lambdaDeployType != LambdaDeployType.CONTAINER) {
            return List.of();
        }
        return getImagesToDeploy(stacksAdded, lambdaDeployType);
    }

    public List<StackDockerImage> getImagesToDeploy(InstanceProperties properties) {
        LambdaDeployType lambdaDeployType = properties.getEnumValue(LAMBDA_DEPLOY_TYPE, LambdaDeployType.class);
        List<OptionalStack> optionalStacks = properties.getEnumList(OPTIONAL_STACKS, OptionalStack.class);
        return getImagesToDeploy(optionalStacks, lambdaDeployType);
    }

    private List<StackDockerImage> getImagesToDeploy(Collection<OptionalStack> stacks, LambdaDeployType lambdaDeployType) {
        return Stream.concat(
                stacks.stream()
                        .map(this::getStackImage)
                        .flatMap(Optional::stream),
                lambdaImages(stacks, lambdaDeployType))
                .collect(toUnmodifiableList());
    }

    private Stream<StackDockerImage> lambdaImages(Collection<OptionalStack> stacks, LambdaDeployType lambdaDeployType) {
        if (lambdaDeployType != LambdaDeployType.CONTAINER) {
            return Stream.empty();
        }
        return lambdaJars.stream().filter(lambda -> lambda.isDeployed(stacks)).map(StackDockerImage::lambdaImage);
    }

    private Optional<StackDockerImage> getStackImage(OptionalStack stack) {
        return Optional.ofNullable(imageByStack.get(stack));
    }

    public Optional<String> getInstanceIdFromRepoName(String repositoryName) {
        return imageByStack.values().stream()
                .filter(image -> repositoryName.endsWith("/" + image.getImageName()))
                .map(image -> repositoryName.substring(0, repositoryName.indexOf("/")))
                .findFirst();
    }
}
