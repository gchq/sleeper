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
import sleeper.clients.util.CommandPipeline;
import sleeper.clients.util.CommandPipelineRunner;
import sleeper.clients.util.EcrRepositoryCreator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static sleeper.clients.util.Command.command;
import static sleeper.clients.util.CommandPipeline.pipeline;

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
        upload(ClientUtils::runCommandInheritIO, request);
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

        if (stacksToBuild.stream().anyMatch(StackDockerImage::isBuildx)) {
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

            Map<String, String> buildArgs = new LinkedHashMap<>();
            stackImage.getLambdaHandler().ifPresent(handler -> {
                buildArgs.put("handler", handler.getHandler());
                copyFile.copyWrappingExceptions(
                        jarsDirectory.resolve(handler.getJar().getFilename()),
                        dockerfileDirectory.resolve("lambda.jar"));
            });

            try {
                if (stackImage.isCreateEmrServerlessPolicy()) {
                    ecrClient.createEmrServerlessAccessPolicy(repositoryName);
                }
                if (stackImage.isBuildx()) {
                    runCommand.runOrThrow(getBuildxCommand(dockerfileDirectory, tag, buildArgs));
                } else {
                    runCommand.runOrThrow(getBuildCommand(dockerfileDirectory, tag, buildArgs));
                    runCommand.runOrThrow("docker", "push", tag);
                }
            } catch (Exception e) {
                ecrClient.deleteRepository(repositoryName);
                throw e;
            }
        }
    }

    private CommandPipeline getBuildxCommand(Path dockerfileDirectory, String tag, Map<String, String> buildArgs) {
        return getBuildCommand(dockerfileDirectory, List.of("docker", "buildx", "build", "--platform", "linux/amd64,linux/arm64", "-t", tag, "--push"), buildArgs);
    }

    private CommandPipeline getBuildCommand(Path dockerfileDirectory, String tag, Map<String, String> buildArgs) {
        return getBuildCommand(dockerfileDirectory, List.of("docker", "build", "-t", tag), buildArgs);
    }

    private CommandPipeline getBuildCommand(Path dockerfileDirectory, List<String> buildCommand, Map<String, String> buildArgs) {
        List<String> command = new ArrayList<>(buildCommand);
        buildArgs.forEach((arg, value) -> command.addAll(List.of("--build-arg", arg + "=" + value)));
        command.add(dockerfileDirectory.toString());
        return pipeline(command(command.toArray(String[]::new)));
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
