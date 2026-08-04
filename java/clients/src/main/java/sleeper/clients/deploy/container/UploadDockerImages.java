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

    // A throwaway local registry used to serve base images to the buildx builder during a build. It is not a
    // deployment registry: nothing is ever deployed or run from it, and it is torn down at the end of the build.
    private static final String LOCAL_REGISTRY_PORT = "5000";
    private static final String LOCAL_REGISTRY_HOST = "localhost:" + LOCAL_REGISTRY_PORT;
    private static final String LOCAL_REGISTRY_CONTAINER = "sleeper-base-registry";

    private final Path baseDockerDirectory;
    private final Path jarsDirectory;
    private final DeployConfiguration deployConfig;
    private final CommandPipelineRunner commandRunner;
    private final CopyFile copyFile;
    private final CopyContainerImage copyImage;
    private final StackDockerImage baseImage;
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
        version = requireNonNull(builder.version, "version must not be null");
        createMultiplatformBuilder = builder.createMultiplatformBuilder;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static UploadDockerImages fromScriptsDirectory(Path scriptsDirectory, EcrClient ecrClient) throws IOException {
        return builder()
                .scriptsDirectory(scriptsDirectory)
                .deployConfig(DeployConfiguration.fromScriptsDirectory(scriptsDirectory))
                .copyImage(CopyContainerImage.withTransferManager(ecrClient))
                .build();
    }

    public boolean isDockerCli() {
        // Only local builds are done with the Docker CLI
        return deployConfig.dockerImageLocation() == DockerImageLocation.LOCAL_BUILD;
    }

    public static void useBuildXBuilder(CommandPipelineRunner commandRunner) throws IOException, InterruptedException {
        createBuildXBuilder(commandRunner, false);
    }

    private static void createBuildXBuilder(CommandPipelineRunner commandRunner, boolean useHostNetwork) throws IOException, InterruptedException {
        if (useHostNetwork) {
            // Host networking lets the containerised buildx builder reach the local registry serving base images on
            // localhost. Without it the builder cannot resolve FROM a locally-built base image.
            commandRunner.run("docker", "buildx", "create", "--name", "sleeper", "--driver-opt", "network=host");
        } else {
            commandRunner.run("docker", "buildx", "create", "--name", "sleeper");
        }
        commandRunner.runOrThrow("docker", "buildx", "use", "sleeper");
    }

    public void upload(String repositoryPrefix, List<StackDockerImage> imagesToUpload) throws IOException, InterruptedException {
        if (imagesToUpload.isEmpty()) {
            LOGGER.info("No images need to be built and uploaded, skipping");
            return;
        }
        LOGGER.info("Building and uploading images: {}", imagesToUpload);
        boolean anyUseBaseImage = imagesToUpload.stream().anyMatch(StackDockerImage::isUseDefaultBaseImage);

        if (deployConfig.dockerImageLocation() == DockerImageLocation.LOCAL_BUILD) {
            // A multiplatform image is built in the buildx "sleeper" builder, which resolves a FROM image from a
            // registry and cannot see the local Docker image store. When such an image builds on a base image, we
            // serve base images from a throwaway local registry that both the plain Docker builder and the buildx
            // builder can pull from. When no multiplatform image needs a base, base images are built straight into
            // the local Docker image store, and no registry is needed.
            boolean useLocalRegistry = imagesToUpload.stream()
                    .anyMatch(image -> image.isMultiplatform() && usesBaseImage(image));

            if (createMultiplatformBuilder && (anyUseBaseImage || useLocalRegistry)) {
                createBuildXBuilder(commandRunner, useLocalRegistry);
            }
            if (useLocalRegistry) {
                startLocalRegistry();
            }
            try {
                String baseTag = baseImageTag(repositoryPrefix, baseImage, useLocalRegistry);
                if (anyUseBaseImage) {
                    buildBaseImage(baseTag, baseImage, useLocalRegistry);
                }
                for (StackDockerImage image : imagesToUpload) {
                    Map<String, String> buildArgs = createBuildArgs(repositoryPrefix, image, baseTag, useLocalRegistry);
                    buildAndPushImage(buildTag(repositoryPrefix, image), image, buildArgs);
                }
            } finally {
                if (useLocalRegistry) {
                    stopLocalRegistry();
                }
            }
        } else if (deployConfig.dockerImageLocation() == DockerImageLocation.REPOSITORY) {
            for (StackDockerImage image : imagesToUpload) {
                pullAndPushImage(buildTag(repositoryPrefix, image), image);
            }
        }
    }

    private boolean usesBaseImage(StackDockerImage image) {
        return image.isUseDefaultBaseImage() || image.createOverrideBaseImage(deployConfig).isPresent();
    }

    private void startLocalRegistry() throws IOException, InterruptedException {
        // Best-effort start; tolerate a registry left running by a previous build, as we do for the buildx builder.
        commandRunner.run("docker", "run", "-d", "-p", LOCAL_REGISTRY_PORT + ":" + LOCAL_REGISTRY_PORT,
                "--name", LOCAL_REGISTRY_CONTAINER, "registry:2");
    }

    private void stopLocalRegistry() throws IOException, InterruptedException {
        // Best-effort teardown so a failed build does not mask the original error, and no registry is left running.
        commandRunner.run("docker", "rm", "-f", LOCAL_REGISTRY_CONTAINER);
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

    private void buildBaseImage(String tag, StackDockerImage image, boolean useLocalRegistry) throws IOException, InterruptedException {
        // A base image is only a build input for other images, which resolve it via the BASE_IMAGE build argument. It
        // is never deployed or run directly, so it is never pushed to a deployment registry (e.g. ECR). It is made
        // available either in the local Docker image store, or in a throwaway local registry when a multiplatform
        // build needs it (the buildx builder cannot see the local image store).
        Path dockerfileDirectory = image.resolveBuildContext(baseDockerDirectory, deployConfig);
        if (image.isMultiplatform()) {
            String platformList = ContainerPlatform.buildPlatformListArgument(image.getPlatforms());
            String loadOrPush = useLocalRegistry ? "--push" : "--load";
            commandRunner.runOrThrow(dockerBuild(
                    List.of("docker", "buildx", "build"),
                    List.of("--platform", platformList, loadOrPush, "-t", tag),
                    dockerfileDirectory));
        } else {
            commandRunner.runOrThrow(dockerBuild(
                    List.of("docker", "build"),
                    List.of("-t", tag),
                    dockerfileDirectory));
            if (useLocalRegistry) {
                commandRunner.runOrThrow("docker", "push", tag);
            }
        }
    }

    private String baseImageTag(String repositoryPrefix, StackDockerImage image, boolean useLocalRegistry) {
        if (useLocalRegistry) {
            return LOCAL_REGISTRY_HOST + "/" + image.getImageName() + ":" + version;
        } else {
            return buildTag(repositoryPrefix, image);
        }
    }

    private Map<String, String> createBuildArgs(String repositoryPrefix, StackDockerImage image, String baseTag, boolean useLocalRegistry) throws IOException, InterruptedException {
        StackDockerImage overrideBaseImage = image.createOverrideBaseImage(deployConfig).orElse(null);
        if (overrideBaseImage != null) {
            String overrideBaseTag = baseImageTag(repositoryPrefix, overrideBaseImage, useLocalRegistry);
            buildBaseImage(overrideBaseTag, overrideBaseImage, useLocalRegistry);
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
