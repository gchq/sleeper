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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.deploy.DeployConfiguration;
import sleeper.clients.util.command.CommandPipelineRunner;
import sleeper.clients.util.command.CommandUtils;
import sleeper.core.SleeperVersion;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class UploadDockerImages {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadDockerImages.class);
    private final Path baseDockerDirectory;
    private final Path jarsDirectory;
    private final DeployConfiguration deployConfig;
    private final CommandPipelineRunner commandRunner;
    private final CopyFile copyFile;
    private final String version;
    private final boolean createMultiplatformBuilder;

    private UploadDockerImages(Builder builder) {
        baseDockerDirectory = requireNonNull(builder.baseDockerDirectory, "baseDockerDirectory must not be null");
        jarsDirectory = requireNonNull(builder.jarsDirectory, "jarsDirectory must not be null");
        deployConfig = requireNonNull(builder.deployConfig, "deployConfig must not be null");
        commandRunner = requireNonNull(builder.commandRunner, "commandRunner must not be null");
        copyFile = requireNonNull(builder.copyFile, "copyFile must not be null");
        version = requireNonNull(builder.version, "version must not be null");
        createMultiplatformBuilder = builder.createMultiplatformBuilder;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static UploadDockerImages fromScriptsDirectory(Path scriptsDirectory) throws IOException {
        return builder()
                .scriptsDirectory(scriptsDirectory)
                .deployConfig(DeployConfiguration.fromScriptsDirectory(scriptsDirectory))
                .build();
    }

    public void upload(String repositoryPrefix, List<StackDockerImage> imagesToUpload, UploadDockerImagesCallbacks callbacks) throws IOException, InterruptedException {
        if (imagesToUpload.isEmpty()) {
            LOGGER.info("No images need to be built and uploaded, skipping");
            return;
        } else {
            LOGGER.info("Building and uploading images: {}", imagesToUpload);
        }

        if (deployConfig.dockerImageLocation() == DockerImageLocation.LOCAL_BUILD
                && createMultiplatformBuilder
                && imagesToUpload.stream().anyMatch(StackDockerImage::isMultiplatform)) {
            commandRunner.run("docker", "buildx", "rm", "sleeper");
            commandRunner.runOrThrow("docker", "buildx", "create", "--name", "sleeper", "--use");
        }

        for (StackDockerImage image : imagesToUpload) {
            String tag = buildTag(repositoryPrefix, image);
            if (deployConfig.dockerImageLocation() == DockerImageLocation.LOCAL_BUILD) {
                buildAndPushImage(tag, image);
            } else if (deployConfig.dockerImageLocation() == DockerImageLocation.REPOSITORY) {
                pullAndPushImage(tag, image);
            } else {
                throw new IllegalArgumentException("Unrecognised Docker image location: " + deployConfig.dockerImageLocation());
            }
        }
    }

    private void buildAndPushImage(String tag, StackDockerImage image) throws IOException, InterruptedException {
        Path dockerfileDirectory = baseDockerDirectory.resolve(image.getDirectoryName());
        image.getLambdaJar().ifPresent(jar -> {
            copyFile.copyWrappingExceptions(
                    jarsDirectory.resolve(jar.getFilename()),
                    dockerfileDirectory.resolve("lambda.jar"));
        });

        if (image.isMultiplatform()) {
            commandRunner.runOrThrow("docker", "buildx", "build", "--platform", "linux/amd64,linux/arm64", "-t", tag, "--push", dockerfileDirectory.toString());
        } else {
            commandRunner.runOrThrow("docker", "build", "-t", tag, dockerfileDirectory.toString());
            commandRunner.runOrThrow("docker", "push", tag);
        }
    }

    private void pullAndPushImage(String tag, StackDockerImage image) throws IOException, InterruptedException {
        String sourceTag = buildTag(deployConfig.dockerRepositoryPrefix(), image);
        commandRunner.runOrThrow("docker", "pull", sourceTag);
        commandRunner.runOrThrow("docker", "tag", sourceTag, tag);
        commandRunner.runOrThrow("docker", "push", tag);
    }

    private String buildTag(String repositoryPrefix, StackDockerImage image) {
        return repositoryPrefix + "/" + image.getImageName() + ":" + version;
    }

    public CommandPipelineRunner getCommandRunner() {
        return commandRunner;
    }

    public String getVersion() {
        return version;
    }

    public static final class Builder {
        private Path baseDockerDirectory;
        private Path jarsDirectory;
        private DeployConfiguration deployConfig;
        private CommandPipelineRunner commandRunner = CommandUtils::runCommandInheritIO;
        private CopyFile copyFile = (source, target) -> Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
        private String version = SleeperVersion.getVersion();
        private boolean createMultiplatformBuilder = true;

        private Builder() {
        }

        public Builder scriptsDirectory(Path scriptsDirectory) {
            return baseDockerDirectory(scriptsDirectory.resolve("docker"))
                    .jarsDirectory(scriptsDirectory.resolve("jars"));
        }

        public Builder baseDockerDirectory(Path baseDockerDirectory) {
            this.baseDockerDirectory = baseDockerDirectory;
            return this;
        }

        public Builder jarsDirectory(Path jarsDirectory) {
            this.jarsDirectory = jarsDirectory;
            return this;
        }

        public Builder deployConfig(DeployConfiguration deployConfig) {
            this.deployConfig = deployConfig;
            return this;
        }

        public Builder commandRunner(CommandPipelineRunner commandRunner) {
            this.commandRunner = commandRunner;
            return this;
        }

        public Builder copyFile(CopyFile copyFile) {
            this.copyFile = copyFile;
            return this;
        }

        public Builder version(String version) {
            this.version = version;
            return this;
        }

        public Builder createMultiplatformBuilder(boolean createMultiplatformBuilder) {
            this.createMultiplatformBuilder = createMultiplatformBuilder;
            return this;
        }

        public UploadDockerImages build() {
            return new UploadDockerImages(this);
        }
    }

    public interface CopyFile {

        void copy(Path source, Path target) throws IOException;

        default void copyWrappingExceptions(Path source, Path target) {
            try {
                copy(source, target);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
