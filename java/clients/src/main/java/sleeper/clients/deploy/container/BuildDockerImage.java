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

import sleeper.clients.util.command.CommandPipelineRunner;
import sleeper.clients.util.command.CommandUtils;
import sleeper.core.SleeperVersion;
import sleeper.core.deploy.ContainerPlatform;
import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaJar;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

/**
 * A command line utility to build a Docker image based on the built-in configuration of images in Sleeper.
 */
public class BuildDockerImage {

    private BuildDockerImage() {
    }

    public static final CommandLineUsage USAGE = CommandLineUsage.builder()
            .positionalArguments(List.of("scripts directory", "image name", "tag"))
            .systemArguments(List.of("scripts directory"))
            .options(List.of(
                    CommandOption.longFlag("multiplatform"),
                    CommandOption.longOption("default-base-image")))
            .helpSummary("Available Docker deployment image names: " +
                    DockerDeployment.all().stream().map(DockerDeployment::getDeploymentName).collect(joining(", ")) + "\n\n" +
                    "Available lambda image names: " +
                    LambdaJar.all().stream().map(LambdaJar::getImageName).collect(joining(", ")) + "\n\n" +
                    "The --multiplatform flag specifies to build a multiplatform image if it's configured to be " +
                    "built that way. By default an image is only built for the default platform. If you pass " +
                    "--default-base-image <image>, it will be set in the BASE_IMAGE build argument, but only if " +
                    "the image uses the default base image. Other arguments will be passed through to Docker as " +
                    "options when specified at the end.")
            .passThroughExtraArguments(true)
            .build();

    public static Arguments readArguments(CommandArguments arguments) {
        return new Arguments(
                Path.of(arguments.getString("scripts directory")),
                arguments.getString("image name"),
                arguments.getString("tag"),
                arguments.isFlagSet("multiplatform"),
                arguments.getOptionalString("default-base-image").orElse(null),
                arguments.getPassthroughArguments());
    }

    public static void main(String[] rawArgs) throws IOException, InterruptedException {
        Arguments args = CommandArguments.parseAndValidateOrExit(USAGE, rawArgs, arguments -> readArguments(arguments));
        build(DockerImageConfiguration.getDefault(), CommandUtils::runCommandInheritIO, Files::copy, args);
    }

    public static void build(DockerImageConfiguration configuration, CommandPipelineRunner commandRunner, FileCopier fileCopier, Arguments args) throws IOException, InterruptedException {
        StackDockerImage image = configuration.getDockerImageByName(args.imageName()).orElseThrow();
        Path dockerfileDirectory = args.dockerDir().resolve(image.getDirectoryName());

        image.getLambdaJar().ifPresent(jar -> {
            Path copyFrom = args.jarsDir().resolve(jar.getFilename(SleeperVersion.getVersion()));
            Path copyTo = dockerfileDirectory.resolve("lambda.jar");
            try {
                fileCopier.copy(copyFrom, copyTo, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        List<String> dockerCommand = new ArrayList<>();
        List<String> dockerOptions = new ArrayList<>();
        if (image.isUseDefaultBaseImage()) {
            args.defaultBaseImageOpt().ifPresent(base -> dockerOptions.addAll(List.of("--build-arg", "BASE_IMAGE=" + base)));
        }
        if (args.isMultiplatform() && !image.getPlatforms().isEmpty()) {
            UploadDockerImages.useBuildXBuilder(commandRunner);
            String platformList = ContainerPlatform.buildPlatformListArgument(image.getPlatforms());
            dockerOptions.addAll(List.of("--platform", platformList, "--load"));
            dockerCommand.addAll(List.of("docker", "buildx", "build"));
        } else {
            dockerCommand.addAll(List.of("docker", "build"));
        }
        dockerOptions.addAll(List.of("-t", args.tag()));
        dockerOptions.addAll(args.dockerOptions());
        dockerCommand.addAll(dockerOptions);
        dockerCommand.add(dockerfileDirectory.toString());
        commandRunner.runOrThrow(dockerCommand.toArray(String[]::new));
    }

    public interface FileCopier {

        void copy(Path from, Path to, CopyOption... options) throws IOException;
    }

    private record Arguments(Path scriptsDir, String imageName, String tag, boolean isMultiplatform, String defaultBaseImage, List<String> dockerOptions) {

        Path dockerDir() {
            return scriptsDir.resolve("docker");
        }

        Path jarsDir() {
            return scriptsDir.resolve("jars");
        }

        Optional<String> defaultBaseImageOpt() {
            return Optional.ofNullable(defaultBaseImage);
        }
    }

}
