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

import sleeper.clients.util.command.CommandPipelineRunner;
import sleeper.clients.util.command.CommandUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;

public class UploadDockerImages {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadDockerImages.class);
    private final Path baseDockerDirectory;
    private final Path jarsDirectory;
    private final CopyFile copyFile;
    private final EcrRepositoryCreator.Client ecrClient;

    private UploadDockerImages(Builder builder) {
        baseDockerDirectory = requireNonNull(builder.baseDockerDirectory, "baseDockerDirectory must not be null");
        jarsDirectory = requireNonNull(builder.jarsDirectory, "jarsDirectory must not be null");
        copyFile = requireNonNull(builder.copyFile, "copyFile must not be null");
        ecrClient = requireNonNull(builder.ecrClient, "ecrClient must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public void upload(UploadDockerImagesRequest request) throws IOException, InterruptedException {
        upload(CommandUtils::runCommandInheritIO, request);
    }

    public void upload(CommandPipelineRunner runCommand, UploadDockerImagesRequest request) throws IOException, InterruptedException {
        List<StackDockerImage> stacksToUpload = request.getImages();
        LOGGER.info("Images expected: {}", stacksToUpload);
        List<StackDockerImage> stacksToBuild = stacksToUpload.stream()
                .filter(stackDockerImage -> imageDoesNotExistInRepositoryWithVersion(stackDockerImage, request))
                .collect(Collectors.toUnmodifiableList());
        String repositoryHost = String.format("%s.dkr.ecr.%s.amazonaws.com", request.getAccount(), request.getRegion());

        if (stacksToBuild.isEmpty()) {
            LOGGER.info("No images need to be built and uploaded, skipping");
            return;
        } else {
            LOGGER.info("Building and uploading images: {}", stacksToBuild);
            runCommand.runOrThrow(pipeline(
                    command("aws", "ecr", "get-login-password", "--region", request.getRegion()),
                    command("docker", "login", "--username", "AWS", "--password-stdin", repositoryHost)));
        }

        if (stacksToBuild.stream().anyMatch(StackDockerImage::isMultiplatform)) {
            runCommand.run("docker", "buildx", "rm", "sleeper");
            runCommand.runOrThrow("docker", "buildx", "create", "--name", "sleeper", "--use");
        }

        for (StackDockerImage stackImage : stacksToBuild) {
            Path dockerfileDirectory = baseDockerDirectory.resolve(stackImage.getDirectoryName());
            String repositoryName = request.getEcrPrefix() + "/" + stackImage.getImageName();
            if (!ecrClient.repositoryExists(repositoryName)) {
                ecrClient.createRepository(repositoryName);
            }
            String tag = repositoryHost + "/" + repositoryName + ":" + request.getVersion();

            stackImage.getLambdaJar().ifPresent(jar -> {
                copyFile.copyWrappingExceptions(
                        jarsDirectory.resolve(jar.getFilename()),
                        dockerfileDirectory.resolve("lambda.jar"));
            });

            try {
                if (stackImage.isCreateEmrServerlessPolicy()) {
                    ecrClient.createEmrServerlessAccessPolicy(repositoryName);
                }
                if (stackImage.isMultiplatform()) {
                    runCommand.runOrThrow("docker", "buildx", "build", "--platform", "linux/amd64,linux/arm64", "-t", tag, "--push", dockerfileDirectory.toString());
                } else {
                    runCommand.runOrThrow("docker", "build", "-t", tag, dockerfileDirectory.toString());
                    runCommand.runOrThrow("docker", "push", tag);
                }
            } catch (Exception e) {
                ecrClient.deleteRepository(repositoryName);
                throw e;
            }
        }
    }

    private boolean imageDoesNotExistInRepositoryWithVersion(
            StackDockerImage stackDockerImage, UploadDockerImagesRequest request) {
        String imagePath = request.getEcrPrefix() + "/" + stackDockerImage.getImageName();
        if (ecrClient.versionExistsInRepository(imagePath, request.getVersion())) {
            LOGGER.info("Stack image {} already exists in ECR with version {}",
                    stackDockerImage.getImageName(), request.getVersion());
            return false;
        } else {
            LOGGER.info("Stack image {} does not exist in ECR with version {}",
                    stackDockerImage.getImageName(), request.getVersion());
            return true;
        }
    }

    public static final class Builder {
        private Path baseDockerDirectory;
        private Path jarsDirectory;
        private CopyFile copyFile = (source, target) -> Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
        private EcrRepositoryCreator.Client ecrClient;

        private Builder() {
        }

        public Builder baseDockerDirectory(Path baseDockerDirectory) {
            this.baseDockerDirectory = baseDockerDirectory;
            return this;
        }

        public Builder jarsDirectory(Path jarsDirectory) {
            this.jarsDirectory = jarsDirectory;
            return this;
        }

        public Builder copyFile(CopyFile copyFile) {
            this.copyFile = copyFile;
            return this;
        }

        public Builder ecrClient(EcrRepositoryCreator.Client ecrClient) {
            this.ecrClient = ecrClient;
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
