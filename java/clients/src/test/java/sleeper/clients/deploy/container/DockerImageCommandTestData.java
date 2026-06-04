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

import sleeper.clients.util.command.CommandPipeline;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.ArrayList;
import java.util.List;

import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DNS_SUFFIX;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;

public class DockerImageCommandTestData {

    private DockerImageCommandTestData() {
    }

    private static final String DEFAULT_ECR_HOSTNAME = "123.dkr.ecr.test-region.amazonaws.com";

    public static List<CommandPipeline> commandsToLoginDockerAndPushImages(InstanceProperties instanceProperties, String... images) {
        List<CommandPipeline> commands = new ArrayList<>();
        commands.add(dockerLoginToEcrCommand(ecrHostname(instanceProperties)));
        commands.add(createBuildxBuilderInstanceCommand());
        commands.add(useBuildxBuilderInstanceCommand());
        String baseTag = tag(instanceProperties, "base");
        commands.add(buildAndPushMultiplatformImageCommand(baseTag, "./docker/base", baseTag));
        for (String image : images) {
            String tag = tag(instanceProperties, image);
            commands.add(buildImageCommand(tag, "./docker/" + image, baseTag));
            commands.add(pushImageCommand(tag));
        }
        return commands;
    }

    private static String tag(InstanceProperties instanceProperties, String image) {
        return ecrHostname(instanceProperties) + "/" + instanceProperties.get(ECR_REPOSITORY_PREFIX) + "/" + image + ":" + instanceProperties.get(VERSION);
    }

    private static String ecrHostname(InstanceProperties instanceProperties) {
        return instanceProperties.get(ACCOUNT) + ".dkr.ecr." + instanceProperties.get(REGION) + "." + instanceProperties.get(DNS_SUFFIX);
    }

    public static CommandPipeline dockerLoginToEcrCommand() {
        return dockerLoginToEcrCommand(DEFAULT_ECR_HOSTNAME);
    }

    private static CommandPipeline dockerLoginToEcrCommand(String ecrHostname) {
        return pipeline(command("aws", "ecr", "get-login-password", "--region", "test-region"),
                command("docker", "login", "--username", "AWS", "--password-stdin", ecrHostname));
    }

    public static CommandPipeline buildImageCommand(String tag, String dockerDirectory, String baseTag) {
        return pipeline(command("docker", "build", "--build-arg", "BASE_IMAGE=" + baseTag, "-t", tag, dockerDirectory));
    }

    public static CommandPipeline buildLambdaImageCommand(String tag, String dockerDirectory, String baseTag) {
        return pipeline(command("docker", "build", "--provenance=false", "--build-arg", "BASE_IMAGE=" + baseTag, "-t", tag, dockerDirectory));
    }

    public static CommandPipeline pullImageCommand(String tag) {
        return pipeline(command("docker", "pull", tag));
    }

    public static CommandPipeline tagImageCommand(String sourceTag, String targetTag) {
        return pipeline(command("docker", "tag", sourceTag, targetTag));
    }

    public static CommandPipeline pushImageCommand(String tag) {
        return pipeline(command("docker", "push", tag));
    }

    public static CommandPipeline removeOldBuildxBuilderInstanceCommand() {
        return pipeline(command("docker", "buildx", "rm", "sleeper"));
    }

    public static CommandPipeline createBuildxBuilderInstanceCommand() {
        return pipeline(command("docker", "buildx", "create", "--name", "sleeper"));
    }

    public static CommandPipeline useBuildxBuilderInstanceCommand() {
        return pipeline(command("docker", "buildx", "use", "sleeper"));
    }

    public static CommandPipeline buildAndPushMultiplatformImageCommand(String tag, String dockerDirectory, String baseTag) {
        return pipeline(command("docker", "buildx", "build", "--build-arg", "BASE_IMAGE=" + baseTag, "--platform", "linux/amd64,linux/arm64",
                "-t", tag, "--push", dockerDirectory));
    }

}
