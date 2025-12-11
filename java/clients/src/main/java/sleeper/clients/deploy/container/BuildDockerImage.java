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

import sleeper.clients.util.command.CommandPipelineRunner;
import sleeper.clients.util.command.CommandUtils;
import sleeper.core.SleeperVersion;
import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaJar;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.joining;

/**
 * A command line utility to build a Docker image based on the built-in configuration of images in Sleeper.
 */
public class BuildDockerImage {

    private BuildDockerImage() {
    }

    public static void main(String[] rawArgs) throws IOException, InterruptedException {
        CommandLineUsage usage = CommandLineUsage.builder()
                .positionalArguments(List.of("scripts directory", "image name", "tag"))
                .systemArguments(List.of("scripts directory"))
                .options(List.of(CommandOption.longFlag("lambda")))
                .helpSummary("Available Docker deployment image names: " +
                        DockerDeployment.all().stream().map(DockerDeployment::getDeploymentName).collect(joining(", ")) + "\n\n" +
                        "Available lambda image names: " +
                        LambdaJar.all().stream().map(LambdaJar::getImageName).collect(joining(", ")) + "\n\n" +
                        "Other arguments will be passed through to Docker as options when specified at the end.")
                .passThroughExtraArguments(true)
                .build();
        Arguments args = CommandArguments.parseAndValidateOrExit(usage, rawArgs, arguments -> new Arguments(
                Path.of(arguments.getString("scripts directory")),
                arguments.getString("image name"),
                arguments.getString("tag"),
                arguments.isFlagSet("lambda"),
                arguments.getPassthroughArguments()));
        DockerImageConfiguration configuration = DockerImageConfiguration.getDefault();
        CommandPipelineRunner commandRunner = CommandUtils::runCommandInheritIO;

        Path dockerfileDirectory;
        if (args.isLambda()) {
            dockerfileDirectory = args.dockerDir().resolve("lambda");
            LambdaJar jar = configuration.getLambdaJarByImageName(args.imageName()).orElseThrow();
            Path copyFrom = args.jarsDir().resolve(jar.getFilename(SleeperVersion.getVersion()));
            Path copyTo = dockerfileDirectory.resolve("lambda.jar");
            Files.copy(copyFrom, copyTo, StandardCopyOption.REPLACE_EXISTING);
        } else {
            DockerDeployment deployment = configuration.getDockerDeploymentByName(args.imageName()).orElseThrow();
            dockerfileDirectory = args.dockerDir().resolve(deployment.getDeploymentName());
        }

        List<String> dockerCommand = new ArrayList<>();
        dockerCommand.addAll(List.of("docker", "build", "-t", args.tag()));
        dockerCommand.addAll(args.dockerOptions());
        dockerCommand.add(dockerfileDirectory.toString());
        commandRunner.runOrThrow(dockerCommand.toArray(String[]::new));
    }

    private record Arguments(Path scriptsDir, String imageName, String tag, boolean isLambda, List<String> dockerOptions) {

        Path dockerDir() {
            return scriptsDir.resolve("docker");
        }

        Path jarsDir() {
            return scriptsDir.resolve("jars");
        }
    }

}
