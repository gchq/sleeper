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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.util.command.CommandPipelineRunner;
import sleeper.clients.util.command.CommandUtils;
import sleeper.core.SleeperVersion;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class UploadDockerImages {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadDockerImages.class);
    private final CommandPipelineRunner commandRunner;
    private final CopyFile copyFile;
    private final Path baseDockerDirectory;
    private final Path jarsDirectory;
    private final String version;

    private UploadDockerImages(Builder builder) {
        commandRunner = requireNonNull(builder.commandRunner, "commandRunner must not be null");
        copyFile = requireNonNull(builder.copyFile, "copyFile must not be null");
        baseDockerDirectory = requireNonNull(builder.baseDockerDirectory, "baseDockerDirectory must not be null");
        jarsDirectory = requireNonNull(builder.jarsDirectory, "jarsDirectory must not be null");
        version = requireNonNull(builder.version, "version must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static UploadDockerImages fromScriptsDirectory(Path scriptsDirectory) {
        return builder()
                .baseDockerDirectory(scriptsDirectory.resolve("docker"))
                .jarsDirectory(scriptsDirectory.resolve("jars"))
                .build();
    }

    public void upload(String repositoryPrefix, List<StackDockerImage> imagesToUpload, UploadDockerImagesCallbacks callbacks) throws IOException, InterruptedException {
        if (imagesToUpload.isEmpty()) {
            LOGGER.info("No images need to be built and uploaded, skipping");
            return;
        } else {
            LOGGER.info("Building and uploading images: {}", imagesToUpload);
            callbacks.beforeAll();
        }

        for (StackDockerImage image : imagesToUpload) {
            Path dockerfileDirectory = baseDockerDirectory.resolve(image.getDirectoryName());
            String tag = repositoryPrefix + "/" + image.getImageName() + ":" + version;
            callbacks.beforeEach(image);

            image.getLambdaJar().ifPresent(jar -> {
                copyFile.copyWrappingExceptions(
                        jarsDirectory.resolve(jar.getFilename()),
                        dockerfileDirectory.resolve("lambda.jar"));
            });

            try {
                if (image.isMultiplatform()) {
                    commandRunner.runOrThrow("docker", "build", "--platform", "linux/amd64,linux/arm64", "-t", tag, "--push", dockerfileDirectory.toString());
                } else {
                    commandRunner.runOrThrow("docker", "build", "-t", tag, dockerfileDirectory.toString());
                    commandRunner.runOrThrow("docker", "push", tag);
                }
            } catch (Exception e) {
                callbacks.onFail(image, e);
                throw e;
            }
        }
    }

    public CommandPipelineRunner getCommandRunner() {
        return commandRunner;
    }

    public String getVersion() {
        return version;
    }

    public static final class Builder {
        private CommandPipelineRunner commandRunner = CommandUtils::runCommandInheritIO;
        private CopyFile copyFile = (source, target) -> Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
        private Path baseDockerDirectory;
        private Path jarsDirectory;
        private String version = SleeperVersion.getVersion();

        private Builder() {
        }

        public Builder commandRunner(CommandPipelineRunner commandRunner) {
            this.commandRunner = commandRunner;
            return this;
        }

        public Builder copyFile(CopyFile copyFile) {
            this.copyFile = copyFile;
            return this;
        }

        public Builder baseDockerDirectory(Path baseDockerDirectory) {
            this.baseDockerDirectory = baseDockerDirectory;
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
}
