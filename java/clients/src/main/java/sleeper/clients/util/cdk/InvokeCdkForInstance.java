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
package sleeper.clients.util.cdk;

import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.CommandRunner;
import sleeper.core.properties.instance.InstanceProperties;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class InvokeCdkForInstance {

    private final Path propertiesFile;
    private final Path jarsDirectory;
    private final String version;

    public enum Type {
        STANDARD("sleeper.cdk.SleeperCdkApp", InvokeCdkForInstance::cdkJarFile),
        SYSTEM_TEST("sleeper.systemtest.cdk.SystemTestApp", InvokeCdkForInstance::systemTestJarFile),
        SYSTEM_TEST_STANDALONE("sleeper.systemtest.cdk.SystemTestStandaloneApp", InvokeCdkForInstance::systemTestJarFile);

        private final String cdkAppClassName;
        private final Function<InvokeCdkForInstance, Path> getCdkJarFile;

        Type(String cdkAppClassName, Function<InvokeCdkForInstance, Path> getCdkJarFile) {
            this.cdkAppClassName = cdkAppClassName;
            this.getCdkJarFile = getCdkJarFile;
        }
    }

    private InvokeCdkForInstance(Builder builder) {
        propertiesFile = requireNonNull(builder.propertiesFile, "propertiesFile must not be null");
        jarsDirectory = requireNonNull(builder.jarsDirectory, "jarsDirectory must not be null");
        version = requireNonNull(builder.version, "version must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public void invokeInferringType(InstanceProperties instanceProperties, CdkCommand cdkCommand) throws IOException, InterruptedException {
        invoke(inferType(instanceProperties), cdkCommand);
    }

    public void invokeInferringType(InstanceProperties instanceProperties, CdkCommand cdkCommand, CommandRunner runCommand) throws IOException, InterruptedException {
        invoke(inferType(instanceProperties), cdkCommand, runCommand);
    }

    private static Type inferType(InstanceProperties instanceProperties) {
        if (instanceProperties.isAnyPropertySetStartingWith("sleeper.systemtest")) {
            return Type.SYSTEM_TEST;
        } else {
            return Type.STANDARD;
        }
    }

    public void invoke(Type instanceType, CdkCommand cdkCommand) throws IOException, InterruptedException {
        invoke(instanceType, cdkCommand, ClientUtils::runCommandInheritIO);
    }

    public void invoke(Type instanceType, CdkCommand cdkCommand, CommandRunner runCommand) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>(List.of(
                "cdk",
                "-a", String.format("java -cp \"%s\" %s",
                        instanceType.getCdkJarFile.apply(this), instanceType.cdkAppClassName)));
        cdkCommand.getCommand().forEach(command::add);
        command.addAll(List.of("-c", String.format("propertiesfile=%s", propertiesFile)));
        cdkCommand.getArguments().forEach(command::add);
        command.add("*");

        int exitCode = runCommand.run(command.toArray(new String[0]));

        if (exitCode != 0) {
            throw new CdkFailedException(exitCode);
        }
    }

    private Path cdkJarFile() {
        return jarsDirectory.resolve(String.format("cdk-%s.jar", version));
    }

    private Path systemTestJarFile() {
        return jarsDirectory.resolve(String.format("system-test-cdk-%s.jar", version));
    }

    public static final class Builder {
        private Path propertiesFile;
        private Path jarsDirectory;
        private String version;

        private Builder() {
        }

        public Builder propertiesFile(Path propertiesFile) {
            this.propertiesFile = propertiesFile;
            return this;
        }

        public Builder jarsDirectory(Path jarsDirectory) {
            this.jarsDirectory = jarsDirectory;
            return this;
        }

        public Builder version(String version) {
            this.version = version;
            return this;
        }

        public InvokeCdkForInstance build() {
            return new InvokeCdkForInstance(this);
        }
    }
}
