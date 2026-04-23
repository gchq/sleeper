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
package sleeper.clients.util.cdk;

import sleeper.clients.util.command.CommandRunner;
import sleeper.clients.util.command.CommandUtils;
import sleeper.core.SleeperVersion;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.SleeperCdkDeployment;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class InvokeCdk {

    private final Path jarsDirectory;
    private final String version;
    private final CommandRunner runCommand;

    public enum Type {
        STANDARD(SleeperCdkDeployment.STANDARD),
        ARTEFACTS(SleeperCdkDeployment.ARTEFACTS),
        DEMONSTRATION(SleeperCdkDeployment.DEMONSTRATION),
        SYSTEM_TEST_INFRA(SleeperCdkDeployment.SYSTEM_TEST_INFRA);

        private final SleeperCdkDeployment deployment;

        Type(SleeperCdkDeployment deployment) {
            this.deployment = deployment;
        }
    }

    private InvokeCdk(Builder builder) {
        jarsDirectory = requireNonNull(builder.jarsDirectory, "jarsDirectory must not be null");
        version = requireNonNull(builder.version, "version must not be null");
        runCommand = requireNonNull(builder.runCommand, "runCommand must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static InvokeCdk fromScriptsDirectory(Path scriptsDirectory) {
        return builder().scriptsDirectory(scriptsDirectory).build();
    }

    public void invokeInferringType(InstanceProperties instanceProperties, CdkCommand cdkCommand) throws IOException, InterruptedException {
        invoke(inferType(instanceProperties), cdkCommand);
    }

    private static Type inferType(InstanceProperties instanceProperties) {
        if (instanceProperties.isAnyPropertySetStartingWith("sleeper.systemtest")) {
            return Type.DEMONSTRATION;
        } else {
            return Type.STANDARD;
        }
    }

    public void invoke(Type instanceType, CdkCommand cdkCommand) throws IOException, InterruptedException {
        String appClassName = instanceType.deployment.getCdkAppClassName();
        Path jarFile = instanceType.deployment.getCdkJarFile(jarsDirectory, version);
        List<String> command = new ArrayList<>(List.of(
                "cdk",
                "-a", String.format("java -cp \"%s\" %s",
                        jarFile, appClassName)));
        command.addAll(cdkCommand.command());
        command.addAll(cdkCommand.arguments());
        command.add("*");

        int exitCode = runCommand.run(command.toArray(new String[0]));

        if (exitCode != 0) {
            throw new CdkFailedException(exitCode);
        }
    }

    public static final class Builder {
        private Path jarsDirectory;
        private String version = SleeperVersion.getVersion();
        private CommandRunner runCommand = CommandUtils::runCommandInheritIO;

        private Builder() {
        }

        public Builder scriptsDirectory(Path scriptsDirectory) {
            return jarsDirectory(scriptsDirectory.resolve("jars"));
        }

        public Builder jarsDirectory(Path jarsDirectory) {
            this.jarsDirectory = jarsDirectory;
            return this;
        }

        public Builder version(String version) {
            this.version = version;
            return this;
        }

        public Builder runCommand(CommandRunner runCommand) {
            this.runCommand = runCommand;
            return this;
        }

        public InvokeCdk build() {
            return new InvokeCdk(this);
        }
    }
}
