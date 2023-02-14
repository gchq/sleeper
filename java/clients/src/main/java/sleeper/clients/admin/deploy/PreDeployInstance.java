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
package sleeper.clients.admin.deploy;

import com.amazonaws.services.s3.AmazonS3;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.util.ClientUtils;

import java.io.IOException;
import java.nio.file.Path;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VERSION;

public class PreDeployInstance {

    private final AmazonS3 s3;
    private final Path jarsDirectory;
    private final Path baseDockerDirectory;
    private final Path uploadDockerImagesScript;
    private final InstanceProperties instanceProperties;

    private PreDeployInstance(Builder builder) {
        s3 = builder.s3;
        jarsDirectory = builder.jarsDirectory;
        baseDockerDirectory = builder.baseDockerDirectory;
        uploadDockerImagesScript = builder.uploadDockerImagesScript;
        instanceProperties = builder.instanceProperties;
    }

    public static Builder builder() {
        return new Builder();
    }

    public void preDeploy() throws IOException, InterruptedException {
        uploadJars();
        uploadDockerImages();
    }

    private void uploadJars() throws IOException {
        SyncJars.builder()
                .s3(s3)
                .jarsDirectory(jarsDirectory)
                .instanceProperties(instanceProperties)
                .build().sync();
    }

    private void uploadDockerImages() throws IOException, InterruptedException {
        int exitCode = ClientUtils.runCommand(uploadDockerImagesScript.toString(),
                instanceProperties.get(ID),
                String.format("%s.dkr.ecr.%s.amazonaws.com",
                        instanceProperties.get(ACCOUNT), instanceProperties.get(REGION)),
                instanceProperties.get(VERSION),
                instanceProperties.get(OPTIONAL_STACKS),
                baseDockerDirectory.toString());

        if (exitCode != 0) {
            throw new IOException("Failed to upload Docker images");
        }
    }

    public static final class Builder {
        private AmazonS3 s3;
        private Path jarsDirectory;
        private Path baseDockerDirectory;
        private Path uploadDockerImagesScript;
        private InstanceProperties instanceProperties;

        private Builder() {
        }

        public Builder s3(AmazonS3 s3) {
            this.s3 = s3;
            return this;
        }

        public Builder jarsDirectory(Path jarsDirectory) {
            this.jarsDirectory = jarsDirectory;
            return this;
        }

        public Builder baseDockerDirectory(Path baseDockerDirectory) {
            this.baseDockerDirectory = baseDockerDirectory;
            return this;
        }

        public Builder uploadDockerImagesScript(Path uploadDockerImagesScript) {
            this.uploadDockerImagesScript = uploadDockerImagesScript;
            return this;
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public PreDeployInstance build() {
            return new PreDeployInstance(this);
        }
    }
}
