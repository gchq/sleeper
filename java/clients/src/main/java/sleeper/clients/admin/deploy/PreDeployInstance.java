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
import com.amazonaws.services.ecr.model.DescribeRepositoriesRequest;
import com.amazonaws.services.ecr.model.Repository;
import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.util.ClientUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_COMPACTION_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_INGEST_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VERSION;

public class PreDeployInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(PreDeployInstance.class);

    private final AmazonS3 s3;
    private final AmazonECR ecr;
    private final Path jarsDirectory;
    private final Path baseDockerDirectory;
    private final Path uploadDockerImagesScript;
    private final boolean reuploadDockerImages;
    private final InstanceProperties instanceProperties;

    private PreDeployInstance(Builder builder) {
        s3 = builder.s3;
        ecr = builder.ecr;
        jarsDirectory = builder.jarsDirectory;
        baseDockerDirectory = builder.baseDockerDirectory;
        uploadDockerImagesScript = builder.uploadDockerImagesScript;
        reuploadDockerImages = builder.reuploadDockerImages;
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
                .bucketName(instanceProperties.get(JARS_BUCKET))
                .build().sync();
    }

    private void uploadDockerImages() throws IOException, InterruptedException {
        if (!reuploadDockerImages && dockerRepositoriesPresent()) {
            LOGGER.info("Not reuploading Docker images");
            return;
        }
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

    private boolean dockerRepositoriesPresent() {
        Set<String> dockerRepositoryNames = streamDockerRepositories()
                .map(Repository::getRepositoryName).collect(Collectors.toSet());
        return dockerRepositoryNames.containsAll(List.of(
                instanceProperties.get(ECR_INGEST_REPO),
                instanceProperties.get(ECR_COMPACTION_REPO)));
    }

    private Stream<Repository> streamDockerRepositories() {
        return Stream.iterate(ecr.describeRepositories(new DescribeRepositoriesRequest()),
                        Objects::nonNull,
                        result -> null != result.getNextToken()
                                ? ecr.describeRepositories(new DescribeRepositoriesRequest()
                                .withNextToken(result.getNextToken()))
                                : null)
                .flatMap(result -> result.getRepositories().stream());
    }

    public static final class Builder {
        private AmazonS3 s3;
        private AmazonECR ecr;
        private Path jarsDirectory;
        private Path baseDockerDirectory;
        private Path uploadDockerImagesScript;
        private boolean reuploadDockerImages;
        private InstanceProperties instanceProperties;

        private Builder() {
        }

        public Builder s3(AmazonS3 s3) {
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
