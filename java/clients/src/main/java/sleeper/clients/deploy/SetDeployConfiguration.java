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
package sleeper.clients.deploy;

import sleeper.clients.deploy.container.DockerImageLocation;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.container.images.ContainerRegistryCredentials;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * A command line tool to set the deployment configuration for Sleeper. Saves the configuration to the local file
 * system.
 */
public class SetDeployConfiguration {

    private SetDeployConfiguration() {
    }

    public static void main(String[] args) throws IOException {
        writeConfigurationFile(new ConsoleInput(System.console()), args);
    }

    public static void writeConfigurationFile(ConsoleInput input, String... args) throws IOException {
        CommandLineUsage usage = CommandLineUsage.builder()
                .systemArguments(List.of("scripts directory"))
                .options(List.of(
                        CommandOption.longOption("image-location"),
                        CommandOption.longOption("image-repository-prefix"),
                        CommandOption.longOption("image-username")))
                .helpSummary("" +
                        "Sets the configuration to deploy Sleeper. Saves the configuration to the local file system.\n" +
                        "\n" +
                        "--image-location <location>\n" +
                        "Sets the location of container images to deploy in an instance of Sleeper.\n" +
                        "Available options: " + DockerImageLocation.describeOptions() + "\n" +
                        "\n" +
                        "--image-repository-prefix <prefix>\n" +
                        "The shared prefix for container images in a remote registry to deploy in an instance of " +
                        "Sleeper.\n" +
                        "Implies \"--image-location repository\".\n" +
                        "See the Sleeper documentation for the images that should be found under this prefix:\n" +
                        "https://github.com/gchq/sleeper/blob/develop/docs/deployment/docker-images.md\n" +
                        "\n" +
                        "--image-username <username>\n" +
                        "Sets the username to authenticate with the container image registry. If this is set, the " +
                        "password will be read from stdin as a prompt. If this is not set, no authentication will be " +
                        "sent when communicating with the registry. Important: the credentials will be stored in " +
                        "plain text in the local file system.")
                .build();
        Arguments arguments = CommandArguments.parseAndValidateOrExit(usage, args, commandArgs -> new Arguments(
                Path.of(commandArgs.getString("scripts directory")),
                readConfiguration(input, commandArgs)));

        Files.writeString(
                arguments.scriptsDirectory().resolve("templates").resolve("deployConfig.json"),
                new DeployConfigurationSerDe().toJson(arguments.configuration()));
    }

    private static DeployConfiguration readConfiguration(ConsoleInput input, CommandArguments arguments) {
        String imagePrefix = arguments.getOptionalString("image-repository-prefix").orElse(null);
        String imageUsername = arguments.getOptionalString("image-username").orElse(null);
        DockerImageLocation imageLocation = arguments.getOptionalString("image-location")
                .map(DockerImageLocation::parseOrNull)
                .orElseGet(() -> {
                    if (imagePrefix != null) {
                        return DockerImageLocation.REPOSITORY;
                    } else {
                        throw new IllegalArgumentException("Container image location was not set, use --help for more information.");
                    }
                });
        ContainerRegistryCredentials imageCredentials = imageCredentials(input, imageUsername);
        return new DeployConfiguration(imageLocation, imagePrefix, imageCredentials);
    }

    private static ContainerRegistryCredentials imageCredentials(ConsoleInput input, String imageUsername) {
        if (imageUsername == null) {
            return null;
        }
        String password = input.promptPassword("Please enter the image registry password: ");
        return new ContainerRegistryCredentials(imageUsername, password);
    }

    private record Arguments(Path scriptsDirectory, DeployConfiguration configuration) {
    }

}
