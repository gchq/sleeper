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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.RunCommand;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.SleeperVersion;

import java.io.IOException;
import java.nio.file.Path;

import static java.util.Objects.requireNonNull;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;

public class UploadDockerImages {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadDockerImages.class);

    private final Path baseDockerDirectory;
    private final Path uploadDockerImagesScript;
    private final boolean skip;
    private final InstanceProperties instanceProperties;

    private UploadDockerImages(Builder builder) {
        baseDockerDirectory = requireNonNull(builder.baseDockerDirectory, "baseDockerDirectory must not be null");
        uploadDockerImagesScript = requireNonNull(builder.uploadDockerImagesScript, "uploadDockerImagesScript must not be null");
        skip = builder.skip;
        instanceProperties = requireNonNull(builder.instanceProperties, "instanceProperties must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public void upload() throws IOException, InterruptedException {
        upload(ClientUtils::runCommand);
    }

    public void upload(RunCommand runCommand) throws IOException, InterruptedException {
        if (skip) {
            LOGGER.info("Not uploading Docker images");
            return;
        }
        int exitCode = runCommand.run(uploadDockerImagesScript.toString(),
                instanceProperties.get(ID),
                String.format("%s.dkr.ecr.%s.amazonaws.com",
                        instanceProperties.get(ACCOUNT), instanceProperties.get(REGION)),
                SleeperVersion.getVersion(),
                instanceProperties.get(OPTIONAL_STACKS),
                baseDockerDirectory.toString());

        if (exitCode != 0) {
            throw new IOException("Failed to upload Docker images");
        }
    }

    public static final class Builder {
        private Path baseDockerDirectory;
        private Path uploadDockerImagesScript;
        private boolean skip;
        private InstanceProperties instanceProperties;

        private Builder() {
        }

        public Builder baseDockerDirectory(Path baseDockerDirectory) {
            this.baseDockerDirectory = baseDockerDirectory;
            return this;
        }

        public Builder uploadDockerImagesScript(Path uploadDockerImagesScript) {
            this.uploadDockerImagesScript = uploadDockerImagesScript;
            return this;
        }

        public Builder skipIf(boolean skip) {
            this.skip = skip;
            return this;
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public UploadDockerImages build() {
            return new UploadDockerImages(this);
        }
    }
}
