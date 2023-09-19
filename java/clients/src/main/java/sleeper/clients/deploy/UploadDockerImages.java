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

    public void upload(DockerCommandData data) throws IOException, InterruptedException {
        upload(ClientUtils::runCommandInheritIO, data);
    }

    public void upload(CommandPipelineRunner runCommand, DockerCommandData data) throws IOException, InterruptedException {
        String repositoryHost = String.format("%s.dkr.ecr.%s.amazonaws.com", data.getAccount(), data.getRegion());
        List<StackDockerImage> stacksToBuild = data.getStacks().stream()
                .flatMap(stack -> dockerImageConfig.getStackImage(stack).stream())
                .filter(stackDockerImage -> repositoryExistsWithVersion(stackDockerImage, data))
                .collect(Collectors.toUnmodifiableList());

        if (!stacksToBuild.isEmpty()) {
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

    private boolean repositoryExistsWithVersion(StackDockerImage stackDockerImage, DockerCommandData data) {
        String repositoryName = data.getEcrPrefix() + "/" + stackDockerImage.getImageName();
        return !ecrClient.versionExistsInRepository(repositoryName, data.getVersion());
    }

    public DockerImageConfiguration getDockerImageConfig() {
        return dockerImageConfig;
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
