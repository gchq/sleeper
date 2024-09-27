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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.CommandPipelineRunner;
import sleeper.clients.util.EcrRepositoryCreator;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static sleeper.clients.util.Command.command;
import static sleeper.clients.util.CommandPipeline.pipeline;

public class UploadDockerImages {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadDockerImages.class);
    private final Path baseDockerDirectory;
    private final EcrRepositoryCreator.Client ecrClient;
    private final DockerImageConfiguration dockerImageConfig;

    private UploadDockerImages(Builder builder) {
        baseDockerDirectory = requireNonNull(builder.baseDockerDirectory, "baseDockerDirectory must not be null");
        ecrClient = requireNonNull(builder.ecrClient, "ecrClient must not be null");
        dockerImageConfig = requireNonNull(builder.dockerImageConfig, "dockerImageConfig must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public void upload(StacksForDockerUpload data) throws IOException, InterruptedException {
        upload(ClientUtils::runCommandInheritIO, data, List.of());
    }

    public void upload(CommandPipelineRunner runCommand, StacksForDockerUpload data) throws IOException, InterruptedException {
        upload(runCommand, data, List.of());
    }

    public void upload(CommandPipelineRunner runCommand, StacksForDockerUpload data, List<StackDockerImage> extraDockerImages) throws IOException, InterruptedException {
        LOGGER.info("Optional stacks enabled: {}", data.getStacks());
        String repositoryHost = String.format("%s.dkr.ecr.%s.amazonaws.com", data.getAccount(), data.getRegion());
        List<StackDockerImage> stacksToUpload = dockerImageConfig.getStacksToDeploy(data.getStacks(), extraDockerImages);
        List<StackDockerImage> stacksToBuild = stacksToUpload.stream()
                .filter(stackDockerImage -> imageDoesNotExistInRepositoryWithVersion(stackDockerImage, data))
                .collect(Collectors.toUnmodifiableList());

        if (stacksToBuild.isEmpty()) {
            LOGGER.info("No images need to be built and uploaded, skipping");
            return;
        } else {
            runCommand.runOrThrow(pipeline(
                    command("aws", "ecr", "get-login-password", "--region", data.getRegion()),
                    command("docker", "login", "--username", "AWS", "--password-stdin", repositoryHost)));
        }

        if (stacksToBuild.stream().anyMatch(StackDockerImage::isBuildx)) {
            runCommand.run("docker", "buildx", "rm", "sleeper");
            runCommand.runOrThrow("docker", "buildx", "create", "--name", "sleeper", "--use");
        }

        for (StackDockerImage stackImage : stacksToBuild) {
            String directory = baseDockerDirectory.resolve(stackImage.getDirectoryName()).toString();
            String repositoryName = data.getEcrPrefix() + "/" + stackImage.getImageName();
            if (!ecrClient.repositoryExists(repositoryName)) {
                ecrClient.createRepository(repositoryName);
            }

            String tag = repositoryHost + "/" + repositoryName + ":" + data.getVersion();
            try {
                if (stackImage.isCreateEmrServerlessPolicy()) {
                    ecrClient.createEmrServerlessAccessPolicy(repositoryName);
                }
                if (stackImage.isBuildx()) {
                    runCommand.runOrThrow("docker", "buildx", "build",
                            "--platform", "linux/amd64,linux/arm64",
                            "-t", tag, "--push", directory);
                } else {
                    runCommand.runOrThrow("docker", "build", "-t", tag, directory);
                    runCommand.runOrThrow("docker", "push", tag);
                }
            } catch (Exception e) {
                ecrClient.deleteRepository(repositoryName);
                throw e;
            }
        }
    }

    private boolean imageDoesNotExistInRepositoryWithVersion(StackDockerImage stackDockerImage, StacksForDockerUpload data) {
        String imagePath = data.getEcrPrefix() + "/" + stackDockerImage.getImageName();
        if (ecrClient.versionExistsInRepository(imagePath, data.getVersion())) {
            LOGGER.info("Stack image {} already exists in ECR with version {}",
                    stackDockerImage.getImageName(), data.getVersion());
            return false;
        } else {
            LOGGER.info("Stack image {} does not exist in ECR with version {}",
                    stackDockerImage.getImageName(), data.getVersion());
            return true;
        }
    }

    public static final class Builder {
        private Path baseDockerDirectory;
        private EcrRepositoryCreator.Client ecrClient;
        private DockerImageConfiguration dockerImageConfig = new DockerImageConfiguration();

        private Builder() {
        }

        public Builder baseDockerDirectory(Path baseDockerDirectory) {
            this.baseDockerDirectory = baseDockerDirectory;
            return this;
        }

        public Builder ecrClient(EcrRepositoryCreator.Client ecrClient) {
            this.ecrClient = ecrClient;
            return this;
        }

        public Builder dockerImageConfig(DockerImageConfiguration dockerImageConfig) {
            this.dockerImageConfig = dockerImageConfig;
            return this;
        }

        public UploadDockerImages build() {
            return new UploadDockerImages(this);
        }
    }
}
