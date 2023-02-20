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

import com.amazonaws.services.ecr.AmazonECR;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.InstanceProperties;

import java.io.IOException;
import java.nio.file.Path;

import static java.util.Objects.requireNonNull;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;

public class PreDeployInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(PreDeployInstance.class);

    private final S3Client s3;
    private final AmazonECR ecr;
    private final Path jarsDirectory;
    private final Path baseDockerDirectory;
    private final Path uploadDockerImagesScript;
    private final boolean reuploadDockerImages;
    private final InstanceProperties instanceProperties;

    private PreDeployInstance(Builder builder) {
        s3 = requireNonNull(builder.s3, "s3 must not be null");
        ecr = requireNonNull(builder.ecr, "ecr must not be null");
        jarsDirectory = requireNonNull(builder.jarsDirectory, "jarsDirectory must not be null");
        baseDockerDirectory = requireNonNull(builder.baseDockerDirectory, "baseDockerDirectory must not be null");
        uploadDockerImagesScript = requireNonNull(builder.uploadDockerImagesScript, "uploadDockerImagesScript must not be null");
        reuploadDockerImages = builder.reuploadDockerImages;
        instanceProperties = requireNonNull(builder.instanceProperties, "instanceProperties must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public void preDeploy() throws IOException, InterruptedException {
        LOGGER.info("Running pre-deployment steps");
        boolean jarsChanged = uploadJars();
        uploadDockerImages(jarsChanged);
    }

    private boolean uploadJars() throws IOException {
        return SyncJars.builder()
                .s3(s3)
                .jarsDirectory(jarsDirectory)
                .bucketName(instanceProperties.get(JARS_BUCKET))
                .build().sync();
    }

    private void uploadDockerImages(boolean jarsChanged) throws IOException, InterruptedException {
        UploadDockerImages.builder().ecr(ecr)
                .baseDockerDirectory(baseDockerDirectory)
                .uploadDockerImagesScript(uploadDockerImagesScript)
                .reupload(jarsChanged || reuploadDockerImages)
                .instanceProperties(instanceProperties)
                .build().upload();
    }

    public static final class Builder {
        private S3Client s3;
        private AmazonECR ecr;
        private Path jarsDirectory;
        private Path baseDockerDirectory;
        private Path uploadDockerImagesScript;
        private boolean reuploadDockerImages;
        private InstanceProperties instanceProperties;

        private Builder() {
        }

        public Builder s3(S3Client s3) {
            this.s3 = s3;
            return this;
        }

        public Builder ecr(AmazonECR ecr) {
            this.ecr = ecr;
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

        public Builder reuploadDockerImages(boolean reuploadDockerImages) {
            this.reuploadDockerImages = reuploadDockerImages;
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
