/*
 * Copyright 2022-2023 Crown Copyright
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

import sleeper.configuration.properties.InstanceProperties;
import sleeper.util.ClientUtils;
import sleeper.util.RunCommand;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class InvokeCdkForInstance {

    private final Path instancePropertiesFile;
    private final Path jarsDirectory;
    private final String version;
    private final boolean ensureNewInstance;
    private final boolean skipVersionCheck;

    public enum Type {
        STANDARD("sleeper.cdk.SleeperCdkApp", InvokeCdkForInstance::cdkJarFile),
        SYSTEM_TEST("sleeper.systemtest.cdk.SystemTestApp", InvokeCdkForInstance::systemTestJarFile);
        private final String cdkAppClassName;
        private final Function<InvokeCdkForInstance, Path> getCdkJarFile;

        Type(String cdkAppClassName, Function<InvokeCdkForInstance, Path> getCdkJarFile) {
            this.cdkAppClassName = cdkAppClassName;
            this.getCdkJarFile = getCdkJarFile;
        }
    }

    private InvokeCdkForInstance(Builder builder) {
        instancePropertiesFile = requireNonNull(builder.instancePropertiesFile, "instancePropertiesFile must not be null");
        jarsDirectory = requireNonNull(builder.jarsDirectory, "jarsDirectory must not be null");
        version = requireNonNull(builder.version, "version must not be null");
        ensureNewInstance = builder.ensureNewInstance;
        skipVersionCheck = builder.skipVersionCheck;
    }

    public static Builder deploy() {
        return builder();
    }

    private static Builder builder() {
        return new Builder();
    }

    public void invoke(Type type) throws IOException, InterruptedException {
        invoke(type, ClientUtils::runCommand);
    }

    public void invokeInferringType(InstanceProperties instanceProperties) throws IOException, InterruptedException {
        invokeInferringType(instanceProperties, ClientUtils::runCommand);
    }

    public void invokeInferringType(InstanceProperties instanceProperties, RunCommand runCommand) throws IOException, InterruptedException {
        invoke(inferType(instanceProperties), runCommand);
    }

    private static Type inferType(InstanceProperties instanceProperties) {
        if (instanceProperties.isAnyPropertySetStartingWith("sleeper.systemtest")) {
            return Type.SYSTEM_TEST;
        } else {
            return Type.STANDARD;
        }
    }

    public void invoke(Type instanceType, RunCommand runCommand) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>(List.of(
                "cdk",
                "-a", String.format("java -cp \"%s\" %s",
                        instanceType.getCdkJarFile.apply(this), instanceType.cdkAppClassName),
                "deploy",
                "--require-approval", "never",
                "-c", String.format("propertiesfile=%s", instancePropertiesFile)
        ));
        if (ensureNewInstance) {
            command.addAll(List.of("-c", "newinstance=true"));
        }
        if (skipVersionCheck) {
            command.addAll(List.of("-c", "skipVersionCheck=true"));
        }
        command.add("*");

        int exitCode = runCommand.run(command.toArray(new String[0]));

        if (exitCode != 0) {
            throw new IOException("Failed in cdk deploy");
        }
    }

    private Path cdkJarFile() {
        return jarsDirectory.resolve(String.format("cdk-%s.jar", version));
    }

    private Path systemTestJarFile() {
        return jarsDirectory.resolve(String.format("system-test-%s-utility.jar", version));
    }

    public static final class Builder {
        private Path instancePropertiesFile;
        private Path jarsDirectory;
        private String version;
        private boolean ensureNewInstance;
        private boolean skipVersionCheck;

        private Builder() {
        }

        public Builder instancePropertiesFile(Path instancePropertiesFile) {
            this.instancePropertiesFile = instancePropertiesFile;
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

        public Builder ensureNewInstance(boolean ensureNewInstance) {
            this.ensureNewInstance = ensureNewInstance;
            return this;
        }

        public Builder skipVersionCheck(boolean skipVersionCheck) {
            this.skipVersionCheck = skipVersionCheck;
            return this;
        }

        public InvokeCdkForInstance build() {
            return new InvokeCdkForInstance(this);
        }
    }
}
