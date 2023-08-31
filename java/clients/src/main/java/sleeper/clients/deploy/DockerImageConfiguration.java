/*
 * Copyright 2022-2023 Crown Copyright
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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.clients.deploy.StackDockerImage.dockerBuildImage;
import static sleeper.clients.deploy.StackDockerImage.dockerBuildxImage;

public class DockerImageConfiguration {
    private static final Map<String, StackDockerImage> DEFAULT_DOCKER_IMAGE_BY_STACK = Stream.of(
            dockerBuildImage("IngestStack", "ingest"),
            dockerBuildImage("EksBulkImportStack", "bulk-import-runner"),
            dockerBuildxImage("CompactionStack", "compaction-job-execution"),
            dockerBuildImage("SystemTestStack", "system-test"),
            dockerBuildImage("EmrServerlessBulkImportStack", "bulk-import-runner-emr-serverless")
    ).collect(Collectors.toMap(StackDockerImage::getStackName, image -> image));

    private final Map<String, StackDockerImage> imageByStack;

    public DockerImageConfiguration() {
        this(DEFAULT_DOCKER_IMAGE_BY_STACK);
    }

    public DockerImageConfiguration(Map<String, StackDockerImage> imageByStack) {
        this.imageByStack = imageByStack;
    }

    public static DockerImageConfiguration from(List<StackDockerImage> images) {
        return new DockerImageConfiguration(images.stream()
                .collect(Collectors.toMap(StackDockerImage::getStackName, image -> image)));
    }

    public Optional<StackDockerImage> getStackImage(String stack) {
        return Optional.ofNullable(imageByStack.get(stack));
    }
}
