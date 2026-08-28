/*
 * Copyright 2022-2026 Crown Copyright
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

import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ecr.EcrClient;

import sleeper.clients.deploy.DeployConfiguration;
import sleeper.clients.util.command.CommandPipeline;
import sleeper.clients.util.command.CommandPipelineRunner;
import sleeper.clients.util.command.CommandUtils;
import sleeper.container.images.ContainerImageTransferManager;
import sleeper.container.images.ContainerImageTransferRequest;
import sleeper.container.images.ContainerRegistryCredentials;
import sleeper.container.images.EcrCredentialRetriever;
import sleeper.core.SleeperVersion;
import sleeper.core.deploy.ContainerPlatform;
import sleeper.core.deploy.DockerDeployment;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;

public class UploadDockerImages {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadDockerImages.class);

    private static final String MULTIPLATFORM_BUILDER_NAME = "sleeper-multiplatform";

    /**
     * The port used by the managed base image registry if one is not configured. This is only used when images are
     * built locally, and is run in a local container.
     */
    public static final int BASE_IMAGE_REGISTRY_PORT = 5000;

    private final Path baseDockerDirectory;
    private final Path jarsDirectory;
    private final DeployConfiguration deployConfig;
    private final CommandPipelineRunner commandRunner;
    private final CopyFile copyFile;
    private final CopyContainerImage copyImage;
    private final StackDockerImage baseImage;
    private final BaseImageDestination baseImageDestination;
    private final String version;
    private final boolean createMultiplatformBuilder;

    private UploadDockerImages(Builder builder) {
        baseDockerDirectory = requireNonNull(builder.baseDockerDirectory, "baseDockerDirectory must not be null");
        jarsDirectory = requireNonNull(builder.jarsDirectory, "jarsDirectory must not be null");
        deployConfig = requireNonNull(builder.deployConfig, "deployConfig must not be null");
        commandRunner = requireNonNull(builder.commandRunner, "commandRunner must not be null");
        copyFile = requireNonNull(builder.copyFile, "copyFile must not be null");
        copyImage = Optional.ofNullable(builder.copyImage).orElseGet(() -> CopyContainerImage.localBuildOnly());
        baseImage = requireNonNull(builder.baseImage, "baseImage must not be null");
        baseImageDestination = requireNonNull(builder.baseImageDestination, "baseImageDestination must not be null");
        version = requireNonNull(builder.version, "version must not be null");
        createMultiplatformBuilder = builder.createMultiplatformBuilder;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates an uploader with configuration loaded from the scripts directory. The ECR client is used to retrieve
     * credentials to upload to ECR.
     *
     * @param  scriptsDirectory the scripts directory
     * @param  ecrClient        the client to authenticate with ECR when copying images to a repository
     * @return                  the uploader
     * @throws IOException      if the deploy configuration could not be read
     */
    public static UploadDockerImages fromScriptsDirectory(Path scriptsDirectory, EcrClient ecrClient) throws IOException {
        return builderWith(scriptsDirectory, ecrClient).build();
    }

    /**
     * Creates a builder with configuration loaded from the scripts directory. The ECR client is used to retrieve
     * credentials to upload to ECR.
     *
     * @param  scriptsDirectory the scripts directory
     * @param  ecrClient        the client to authenticate with ECR when copying images to a repository
     * @return                  the builder
     * @throws IOException      if the deploy configuration could not be read
     */
    public static Builder builderWith(Path scriptsDirectory, EcrClient ecrClient) throws IOException {
        DeployConfiguration deployConfig = DeployConfiguration.fromScriptsDirectory(scriptsDirectory);
        return builder()
                .scriptsDirectory(scriptsDirectory)
                .deployConfig(deployConfig)
                .copyImage(CopyContainerImage.withTransferManager(ecrClient))
                .baseImageDestination(BaseImageDestination.managedRegistry(BASE_IMAGE_REGISTRY_PORT));
    }

    public boolean isDockerCli() {
        // Only local builds are done with the Docker CLI
        return deployConfig.dockerImageLocation() == DockerImageLocation.LOCAL_BUILD;
    }

    /**
     * Activates the builder used for multiplatform builds. If it doesn't exist, a new builder is created.
     *
     * @param  commandRunner        the command runner
     * @throws IOException          if a command could not be run
     * @throws InterruptedException if the thread was interrupted while running a command
     */
    public static void useBuildXBuilder(CommandPipelineRunner commandRunner) throws IOException, InterruptedException {
        // If the builder already exists, creation fails and the existing builder will be used
        commandRunner.run("docker", "buildx", "create", "--name", MULTIPLATFORM_BUILDER_NAME,
                "--driver", "docker-container", "--driver-opt", "network=host");
        commandRunner.runOrThrow("docker", "buildx", "use", MULTIPLATFORM_BUILDER_NAME);
    }

    public void upload(String repositoryPrefix, List<StackDockerImage> imagesToUpload) throws IOException, InterruptedException {
        if (imagesToUpload.isEmpty()) {
            LOGGER.info("No images need to be built and uploaded, skipping");
            return;
        }
        LOGGER.info("Building and uploading images: {}", imagesToUpload);
        boolean anyUseBaseImage = imagesToUpload.stream().anyMatch(StackDockerImage::isUseDefaultBaseImage);

        if (deployConfig.dockerImageLocation() == DockerImageLocation.LOCAL_BUILD) {
            baseImageDestination.createIfMissing(commandRunner);
            if (createMultiplatformBuilder) {
                useBuildXBuilder(commandRunner);
            }

            String baseImagePrefix = baseImageDestination.repositoryPrefix(repositoryPrefix);
            String baseTag = buildTag(baseImagePrefix, baseImage);
            if (anyUseBaseImage) {
                buildAndPushImage(baseTag, baseImage, Map.of());
            }
            for (StackDockerImage image : imagesToUpload) {
                Map<String, String> buildArgs = createBuildArgs(baseImagePrefix, image, baseTag);
                buildAndPushImage(buildTag(repositoryPrefix, image), image, buildArgs);
            }
        } else if (deployConfig.dockerImageLocation() == DockerImageLocation.REPOSITORY) {
            for (StackDockerImage image : imagesToUpload) {
                pullAndPushImage(buildTag(repositoryPrefix, image), image);
            }
        }
    }

    private void buildAndPushImage(String tag, StackDockerImage image, Map<String, String> buildArgs) throws IOException, InterruptedException {
        Path dockerfileDirectory = image.resolveBuildContext(baseDockerDirectory, deployConfig);
        image.getLambdaJar().ifPresent(jar -> {
            copyFile.copyWrappingExceptions(
                    jarsDirectory.resolve(jar.getFilename(version)),
                    dockerfileDirectory.resolve("lambda.jar"));
        });

        if (image.isMultiplatform()) {
            String platformList = ContainerPlatform.buildPlatformListArgument(image.getPlatforms());
            commandRunner.runOrThrow(dockerBuild(
                    List.of("docker", "buildx", "build"),
                    optionsWithBuildArgs(buildArgs, "--platform", platformList, "--push", "-t", tag),
                    dockerfileDirectory));
        } else {
            if (image.getLambdaJar().isPresent()) {
                // At time of writing AWS Lambda does not support images with provenance enabled.
                // See https://docs.aws.amazon.com/lambda/latest/dg/java-image.html
                commandRunner.runOrThrow(dockerBuild(
                        List.of("docker", "build"),
                        optionsWithBuildArgs(buildArgs, "--provenance=false", "-t", tag),
                        dockerfileDirectory));
            } else {
                commandRunner.runOrThrow(dockerBuild(
                        List.of("docker", "build"),
                        optionsWithBuildArgs(buildArgs, "-t", tag),
                        dockerfileDirectory));
            }
            commandRunner.runOrThrow("docker", "push", tag);
        }
    }

    private Map<String, String> createBuildArgs(String baseImagePrefix, StackDockerImage image, String baseTag) throws IOException, InterruptedException {
        StackDockerImage overrideBaseImage = image.createOverrideBaseImage(deployConfig).orElse(null);
        if (overrideBaseImage != null) {
            String overrideBaseTag = buildTag(baseImagePrefix, overrideBaseImage);
            buildAndPushImage(overrideBaseTag, overrideBaseImage, Map.of());
            return Map.of("BASE_IMAGE", overrideBaseTag);
        } else if (image.isUseDefaultBaseImage()) {
            return Map.of("BASE_IMAGE", baseTag);
        } else {
            return Map.of();
        }
    }

    private static List<String> optionsWithBuildArgs(Map<String, String> buildArgs, String... options) {
        return Stream.of(
                buildArgs.entrySet().stream()
                        .flatMap(e -> Stream.of("--build-arg", e.getKey() + "=" + e.getValue())),
                Stream.of(options))
                .flatMap(s -> s)
                .toList();
    }

    private static CommandPipeline dockerBuild(List<String> dockerCommand, List<String> options, Path directory) {
        return pipeline(command(Stream.of(
                dockerCommand.stream(),
                options.stream(),
                Stream.of(directory.toString()))
                .flatMap(s -> s).toArray(String[]::new)));
    }

    private void pullAndPushImage(String tag, StackDockerImage image) throws IOException, InterruptedException {
        String sourceTag = buildTag(deployConfig.dockerRepositoryPrefix(), image);
        copyImage.copy(sourceTag, tag, image.getPlatforms(), deployConfig.dockerCredentials());
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
        private CopyContainerImage copyImage;
        private StackDockerImage baseImage = StackDockerImage.fromDockerDeployment(DockerDeployment.BASE);
        private BaseImageDestination baseImageDestination;
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

        public Builder copyImage(CopyContainerImage copyImage) {
            this.copyImage = copyImage;
            return this;
        }

        public Builder baseImage(StackDockerImage baseImage) {
            this.baseImage = baseImage;
            return this;
        }

        public Builder baseImageDestination(BaseImageDestination baseImageDestination) {
            this.baseImageDestination = baseImageDestination;
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

    public interface CopyContainerImage {

        void copy(String source, String target, List<ContainerPlatform> platforms, ContainerRegistryCredentials sourceCredentials) throws IOException, InterruptedException;

        static CopyContainerImage localBuildOnly() {
            return (source, target, platforms, sourceCredentials) -> {
                throw new UnsupportedOperationException(
                        "Copying container images is not configured correctly, expected to always build images locally.");
            };
        }

        static CopyContainerImage withTransferManager(EcrClient ecrClient) {
            return withTransferManager(ContainerImageTransferManager.builder()
                    .cacheDir(SystemUtils.getUserHomePath().resolve(".cache").resolve("sleeper").resolve("container-cache"))
                    .allowInsecureRegistries(false)
                    .build(), ecrClient);
        }

        static CopyContainerImage withTransferManager(ContainerImageTransferManager transferManager, EcrClient ecrClient) {
            EcrCredentialRetriever ecrCredentialRetriever = new EcrCredentialRetriever(ecrClient);
            return (source, target, platforms, sourceCredentials) -> {
                transferManager.transfer(ContainerImageTransferRequest.builder()
                        .sourceImageReference(source)
                        .targetImageReference(target)
                        .platforms(platforms)
                        .sourceCredentials(sourceCredentials)
                        .targetCredentialsRetriever(ecrCredentialRetriever)
                        .build());
            };
        }
    }
}
