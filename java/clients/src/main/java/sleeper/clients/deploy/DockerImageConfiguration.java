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

import org.apache.commons.lang3.EnumUtils;

import sleeper.configuration.properties.validation.OptionalStack;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.clients.deploy.StackDockerImage.dockerBuildImage;
import static sleeper.clients.deploy.StackDockerImage.dockerBuildxImage;
import static sleeper.clients.deploy.StackDockerImage.emrServerlessImage;

public class DockerImageConfiguration {
    private static final Map<OptionalStack, StackDockerImage> DEFAULT_DOCKER_IMAGE_BY_STACK = Map.of(
            OptionalStack.IngestStack, dockerBuildImage("ingest"),
            OptionalStack.EksBulkImportStack, dockerBuildImage("bulk-import-runner"),
            OptionalStack.CompactionStack, dockerBuildxImage("compaction-job-execution"),
            OptionalStack.EmrServerlessBulkImportStack, emrServerlessImage("bulk-import-runner-emr-serverless"));

    private final Map<OptionalStack, StackDockerImage> imageByStack;

    public DockerImageConfiguration() {
        this(DEFAULT_DOCKER_IMAGE_BY_STACK);
    }

    public DockerImageConfiguration(Map<OptionalStack, StackDockerImage> imageByStack) {
        this.imageByStack = imageByStack;
    }

    public List<StackDockerImage> getStacksToDeploy(Collection<String> stacks) {
        return getStacksToDeploy(stacks, List.of());
    }

    public List<StackDockerImage> getStacksToDeploy(Collection<String> stacks, List<StackDockerImage> extraDockerImages) {
        return Stream.concat(
                stacks.stream()
                        .map(this::getStackImage)
                        .flatMap(Optional::stream),
                extraDockerImages.stream())
                .collect(toUnmodifiableList());
    }

    private Optional<StackDockerImage> getStackImage(String stack) {
        return Optional.ofNullable(imageByStack.get(EnumUtils.getEnumIgnoreCase(OptionalStack.class, stack)));
    }

    public Optional<String> getInstanceIdFromRepoName(String repositoryName) {
        return imageByStack.values().stream()
                .filter(image -> repositoryName.endsWith("/" + image.getImageName()))
                .map(image -> repositoryName.substring(0, repositoryName.indexOf("/")))
                .findFirst();
    }
}
