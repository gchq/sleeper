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

import sleeper.clients.deploy.container.UploadDockerImages.CopyFile;
import sleeper.clients.util.command.CommandPipelineRunner;
import sleeper.clients.util.command.CommandUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import static java.util.Objects.requireNonNull;

public class UploadDockerImagesToEcrTemp {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadDockerImagesToEcrTemp.class);
    private final Path baseDockerDirectory;
    private final Path jarsDirectory;
    private final CopyFile copyFile;
    private final EcrRepositoryCreator.Client ecrClient;

    private UploadDockerImagesToEcrTemp(Builder builder) {
        baseDockerDirectory = requireNonNull(builder.baseDockerDirectory, "baseDockerDirectory must not be null");
        jarsDirectory = requireNonNull(builder.jarsDirectory, "jarsDirectory must not be null");
        copyFile = requireNonNull(builder.copyFile, "copyFile must not be null");
        ecrClient = requireNonNull(builder.ecrClient, "ecrClient must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public void upload(UploadDockerImagesToEcrRequest request) throws IOException, InterruptedException {
        upload(CommandUtils::runCommandInheritIO, request);
    }

    public void upload(CommandPipelineRunner runCommand, UploadDockerImagesToEcrRequest request) throws IOException, InterruptedException {
        UploadDockerImages uploader = UploadDockerImages.builder()
                .commandRunner(runCommand)
                .copyFile(copyFile)
                .baseDockerDirectory(baseDockerDirectory)
                .jarsDirectory(jarsDirectory)
                .version(request.getVersion())
                .build();
        new UploadDockerImagesToEcr(uploader, ecrClient).upload(request);
    }

    public static final class Builder {
        private Path baseDockerDirectory;
        private Path jarsDirectory;
        private CopyFile copyFile = (source, target) -> Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
        private EcrRepositoryCreator.Client ecrClient;

        private Builder() {
        }

        public Builder baseDockerDirectory(Path baseDockerDirectory) {
            this.baseDockerDirectory = baseDockerDirectory;
            return this;
        }

        public Builder jarsDirectory(Path jarsDirectory) {
            this.jarsDirectory = jarsDirectory;
            return this;
        }

        public Builder copyFile(CopyFile copyFile) {
            this.copyFile = copyFile;
            return this;
        }

        public Builder ecrClient(EcrRepositoryCreator.Client ecrClient) {
            this.ecrClient = ecrClient;
            return this;
        }

        public UploadDockerImagesToEcrTemp build() {
            return new UploadDockerImagesToEcrTemp(this);
        }
    }
}
