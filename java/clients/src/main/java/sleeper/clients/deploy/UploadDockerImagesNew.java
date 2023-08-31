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

import sleeper.clients.util.CommandPipelineRunner;
import sleeper.clients.util.EcrRepositoryCreator;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.SleeperVersion;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static sleeper.clients.util.Command.command;
import static sleeper.clients.util.CommandPipeline.pipeline;
import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;

public class UploadDockerImagesNew {
    private final Path baseDockerDirectory;
    private final String id;
    private final String account;
    private final String region;
    private final String version;
    private final List<String> stacks;
    private final EcrRepositoryCreator.Client ecrClient;
    private final DockerImageConfiguration dockerImageConfig;

    private UploadDockerImagesNew(Builder builder) {
        baseDockerDirectory = requireNonNull(builder.baseDockerDirectory, "baseDockerDirectory must not be null");
        id = requireNonNull(builder.id, "id must not be null");
        account = requireNonNull(builder.account, "account must not be null");
        region = requireNonNull(builder.region, "region must not be null");
        version = requireNonNull(builder.version, "version must not be null");
        stacks = requireNonNull(builder.stacks, "stacks must not be null");
        ecrClient = requireNonNull(builder.ecrClient, "ecrClient must not be null");
        dockerImageConfig = requireNonNull(builder.dockerImageConfig, "dockerImageConfig must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public void upload(CommandPipelineRunner runCommand) throws IOException, InterruptedException {
        String repositoryHost = String.format("%s.dkr.ecr.%s.amazonaws.com", account, region);

        List<StackDockerImage> stacksToBuild = stacks.stream()
                .flatMap(stack -> dockerImageConfig.getStackImage(stack).stream())
                .filter(stack -> !ecrClient.repositoryExists(repositoryNameForImage(stack.getImageName())))
                .collect(Collectors.toUnmodifiableList());

        if (!stacksToBuild.isEmpty()) {
            runCommand.runOrThrow(pipeline(
                    command("aws", "ecr", "get-login-password", "--region", region),
                    command("docker", "login", "--username", "AWS", "--password-stdin", repositoryHost)));
        }

        if (stacksToBuild.stream().anyMatch(StackDockerImage::isBuildx)) {
            runCommand.run("docker", "buildx", "rm", "sleeper");
            runCommand.runOrThrow("docker", "buildx", "create", "--name", "sleeper", "--use");
        }

        for (StackDockerImage stackImage : stacksToBuild) {
            String directory = baseDockerDirectory.resolve(stackImage.getDirectoryName()).toString();
            String repositoryName = repositoryNameForImage(stackImage.getImageName());
            ecrClient.createRepository(repositoryName);

            String tag = repositoryHost + "/" + repositoryName + ":" + version;
            try {
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

    private String repositoryNameForImage(String image) {
        return id + "/" + image;
    }

    public static final class Builder {
        private Path baseDockerDirectory;
        private String id;
        private String account;
        private String region;
        private String version = SleeperVersion.getVersion();
        private List<String> stacks;
        private EcrRepositoryCreator.Client ecrClient;
        private DockerImageConfiguration dockerImageConfig = new DockerImageConfiguration();

        private Builder() {
        }

        public Builder baseDockerDirectory(Path baseDockerDirectory) {
            this.baseDockerDirectory = baseDockerDirectory;
            return this;
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            return id(instanceProperties.get(ID))
                    .account(instanceProperties.get(ACCOUNT))
                    .region(instanceProperties.get(REGION))
                    .stacks(instanceProperties.getList(OPTIONAL_STACKS));
        }

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder account(String account) {
            this.account = account;
            return this;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        public Builder version(String version) {
            this.version = version;
            return this;
        }

        public Builder stacks(List<String> stacks) {
            this.stacks = stacks;
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

        public UploadDockerImagesNew build() {
            return new UploadDockerImagesNew(this);
        }
    }
}
