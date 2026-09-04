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

package sleeper.clients.deploy.container;

import sleeper.clients.deploy.DeployConfiguration;
import sleeper.core.deploy.ContainerPlatform;
import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaJar;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Details of a Docker image that is needed to deploy a component of a Sleeper instance.
 */
public class StackDockerImage {

    public static final StackDockerImage DEFAULT_BASE = StackDockerImage.builder()
            .imageName("base")
            .directoryName("base")
            .platforms(DockerDeployment.PLATFORMS)
            .isDefaultBaseImage(true)
            .useDefaultBaseImage(false)
            .build();

    private final String imageName;
    private final String directoryName;
    private final Path overrideDirectory;
    private final List<ContainerPlatform> platforms;
    private final boolean createEmrServerlessPolicy;
    private final boolean isDefaultBaseImage;
    private final boolean useDefaultBaseImage;
    private final LambdaJar lambdaJar;

    private StackDockerImage(Builder builder) {
        imageName = builder.imageName;
        directoryName = builder.directoryName;
        overrideDirectory = builder.overrideDirectory;
        platforms = builder.platforms;
        createEmrServerlessPolicy = builder.createEmrServerlessPolicy;
        isDefaultBaseImage = builder.isDefaultBaseImage;
        useDefaultBaseImage = builder.useDefaultBaseImage;
        lambdaJar = builder.lambdaJar;
    }

    /**
     * Creates an instance of this class for a component that is deployed based on a Docker image. Maps from the
     * definition of its deployment.
     *
     * @param  deployment the Docker deployment
     * @return            the Docker image
     */
    public static StackDockerImage fromDockerDeployment(DockerDeployment deployment) {
        return StackDockerImage.builder()
                .imageName(deployment.getDeploymentName())
                .directoryName(deployment.getDeploymentName())
                .platforms(deployment.getPlatforms())
                .createEmrServerlessPolicy(deployment.isCreateEmrServerlessPolicy())
                .isDefaultBaseImage(deployment.isDefaultBaseImage())
                .useDefaultBaseImage(deployment.isUseDefaultBaseImage())
                .build();
    }

    /**
     * Creates an instance of this class for deployment in AWS Lambda. Maps from the definition of a jar containing
     * lambda handlers. Note that this will not be needed if the lambda is deployed as a jar.
     *
     * @param  lambdaJar the definition of the jar
     * @return           the Docker image
     */
    public static StackDockerImage fromLambdaImage(LambdaJar lambdaJar) {
        return builder()
                .imageName(lambdaJar.getImageName())
                .directoryName("lambda")
                .lambdaJar(lambdaJar)
                .build();
    }

    /**
     * Defines a Docker image to be built from a directory matching its image name with default settings.
     *
     * @param  imageName the image name
     * @return           the Docker image details
     */
    public static StackDockerImage dockerBuildImage(String imageName) {
        return builder().imageName(imageName)
                .directoryName(imageName).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getImageName() {
        return imageName;
    }

    public Path resolveBuildContext(Path baseDockerDirectory, DeployConfiguration deployConfig) {
        if (overrideDirectory != null) {
            return overrideDirectory;
        } else if (isDefaultBaseImage) {
            return deployConfig.overrideBaseImageDirPath()
                    .orElseGet(() -> baseDockerDirectory.resolve(directoryName));
        } else {
            return baseDockerDirectory.resolve(directoryName);
        }
    }

    public Optional<StackDockerImage> createOverrideBaseImage(DeployConfiguration deployConfig) {
        return deployConfig.overrideBaseImageDirPathForImage(imageName)
                .map(overrideDirectory -> builder()
                        .imageName(imageName + "-base")
                        .platforms(platforms)
                        .overrideDirectory(overrideDirectory)
                        .build());
    }

    public String getDirectoryName() {
        return directoryName;
    }

    public boolean isMultiplatform() {
        return platforms.size() > 1;
    }

    public List<ContainerPlatform> getPlatforms() {
        return platforms;
    }

    public boolean isCreateEmrServerlessPolicy() {
        return createEmrServerlessPolicy;
    }

    public boolean isUseDefaultBaseImage() {
        return useDefaultBaseImage;
    }

    public Optional<LambdaJar> getLambdaJar() {
        return Optional.ofNullable(lambdaJar);
    }

    @Override
    public int hashCode() {
        return Objects.hash(imageName, directoryName, platforms, createEmrServerlessPolicy, lambdaJar);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StackDockerImage)) {
            return false;
        }
        StackDockerImage other = (StackDockerImage) obj;
        return Objects.equals(imageName, other.imageName) && Objects.equals(directoryName, other.directoryName) && Objects.equals(platforms, other.platforms)
                && createEmrServerlessPolicy == other.createEmrServerlessPolicy && Objects.equals(lambdaJar, other.lambdaJar);
    }

    @Override
    public String toString() {
        return "StackDockerImage{imageName=" + imageName + ", directoryName=" + directoryName +
                ", platforms=" + platforms + ", createEmrServerlessPolicy=" + createEmrServerlessPolicy +
                ", lambdaJar=" + lambdaJar + "}";
    }

    /**
     * Builds Docker image details.
     */
    public static final class Builder {
        private String imageName;
        private String directoryName;
        private Path overrideDirectory;
        private List<ContainerPlatform> platforms = List.of();
        private boolean createEmrServerlessPolicy;
        private boolean isDefaultBaseImage;
        private boolean useDefaultBaseImage = true;
        private LambdaJar lambdaJar;

        private Builder() {
        }

        /**
         * Sets the name of the Docker image. This is used as a part of the Docker repository name that this image will
         * be uploaded to.
         *
         * @param  imageName the name
         * @return           this builder
         */
        public Builder imageName(String imageName) {
            this.imageName = imageName;
            return this;
        }

        /**
         * Sets the name of the directory that contains the Dockerfile. This will be found underneath the standard
         * directory for Sleeper's build output.
         *
         * @param  directoryName the directory name
         * @return               this builder
         */
        public Builder directoryName(String directoryName) {
            this.directoryName = directoryName;
            return this;
        }

        /**
         * Overrides the local directory that contains the Dockerfile. This should only be used when it is not part of
         * Sleeper's standard build output.
         *
         * @param  overrideDirectory the path to the build directory
         * @return                   this builder
         */
        public Builder overrideDirectory(Path overrideDirectory) {
            this.overrideDirectory = overrideDirectory;
            return this;
        }

        /**
         * Sets which platforms this image should be built and transferred for. An empty list means the default platform
         * of the build/transfer tool will be used.
         *
         * @param  platforms the platforms
         * @return           this builder
         */
        public Builder platforms(List<ContainerPlatform> platforms) {
            this.platforms = platforms;
            return this;
        }

        /**
         * Sets whether the ECR repository needs a policy to let EMR Serverless pull the Docker image.
         *
         * @param  createEmrServerlessPolicy true if the EMR Serverless policy is needed
         * @return                           this builder
         */
        public Builder createEmrServerlessPolicy(boolean createEmrServerlessPolicy) {
            this.createEmrServerlessPolicy = createEmrServerlessPolicy;
            return this;
        }

        /**
         * Sets whether this is the default base image, and required to build others by default.
         *
         * @param  isDefaultBaseImage true if this is the base image
         * @return                    this builder
         */
        public Builder isDefaultBaseImage(boolean isDefaultBaseImage) {
            this.isDefaultBaseImage = isDefaultBaseImage;
            return this;
        }

        /**
         * Sets whether this is built from the default base image. If it is, the BASE_IMAGE build argument will be
         * passed during a build.
         *
         * @param  useDefaultBaseImage true if this is built from the default base image
         * @return                     this builder
         */
        public Builder useDefaultBaseImage(boolean useDefaultBaseImage) {
            this.useDefaultBaseImage = useDefaultBaseImage;
            return this;
        }

        /**
         * Sets which lambda jar the Docker image should include, if any.
         *
         * @param  lambdaJar the lambda jar, or null if none
         * @return           this builder
         */
        public Builder lambdaJar(LambdaJar lambdaJar) {
            this.lambdaJar = lambdaJar;
            return this;
        }

        public StackDockerImage build() {
            return new StackDockerImage(this);
        }
    }
}
