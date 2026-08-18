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
import sleeper.core.properties.model.SleeperInternalCdkApp;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class InvokeCdk {

    @FunctionalInterface
    public interface OutputDirFactory {
        Path create() throws IOException;
    }

    private final Path jarsDirectory;
    private final String version;
    private final CommandRunner runCommand;
    private final OutputDirFactory outputDirFactory;

    private InvokeCdk(Builder builder) {
        jarsDirectory = requireNonNull(builder.jarsDirectory, "jarsDirectory must not be null");
        version = requireNonNull(builder.version, "version must not be null");
        runCommand = requireNonNull(builder.runCommand, "runCommand must not be null");
        outputDirFactory = requireNonNull(builder.outputDirFactory, "outputDirFactory must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static InvokeCdk fromScriptsDirectory(Path scriptsDirectory) {
        return builder().scriptsDirectory(scriptsDirectory).build();
    }

    public void invoke(SleeperInternalCdkApp cdkApp, CdkCommand cdkCommand) throws IOException, InterruptedException {
        String appClassName = cdkApp.getCdkAppClassName();
        Path jarFile = cdkApp.getCdkJarFile(jarsDirectory, version);
        // Use a unique temp directory for CDK output so concurrent deployments don't conflict
        // on cdk.out. CDK 2.x locks cdk.out and fails if another process is using it.
        Path outputDir = outputDirFactory.create();
        try {
            List<String> command = new ArrayList<>(List.of(
                    "cdk",
                    "--output", outputDir.toString(),
                    "-a", String.format("java -cp \"%s\" %s",
                            jarFile, appClassName)));
            command.addAll(cdkCommand.command());
            command.addAll(cdkCommand.arguments());
            command.add("*");

            int exitCode = runCommand.run(command.toArray(new String[0]));

            if (exitCode != 0) {
                throw new CdkFailedException(exitCode);
            }
        } finally {
            deleteQuietly(outputDir);
        }
    }

    private static void deleteQuietly(Path dir) {
        try (var stream = Files.walk(dir).sorted(Comparator.reverseOrder())) {
            stream.forEach(p -> p.toFile().delete());
        } catch (IOException ignored) {
        }
    }

    public static final class Builder {
        private Path jarsDirectory;
        private String version = SleeperVersion.getVersion();
        private CommandRunner runCommand = CommandUtils::runCommandInheritIO;
        private OutputDirFactory outputDirFactory = () -> Files.createTempDirectory("sleeper-cdk");

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

        public Builder outputDirFactory(OutputDirFactory outputDirFactory) {
            this.outputDirFactory = outputDirFactory;
            return this;
        }

        public InvokeCdk build() {
            return new InvokeCdk(this);
        }
    }
}
