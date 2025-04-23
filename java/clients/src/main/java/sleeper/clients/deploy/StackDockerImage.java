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

package sleeper.clients.deploy;

import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaJar;

import java.util.Objects;
import java.util.Optional;

/**
 * Details of a Docker image that is needed to deploy a component of a Sleeper instance.
 */
public class StackDockerImage {
    private final String imageName;
    private final String directoryName;
    private final boolean multiplatform;
    private final boolean createEmrServerlessPolicy;
    private final LambdaJar lambdaJar;

    private StackDockerImage(Builder builder) {
        imageName = builder.imageName;
        directoryName = builder.directoryName;
        multiplatform = builder.multiplatform;
        createEmrServerlessPolicy = builder.createEmrServerlessPolicy;
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
                .multiplatform(deployment.isMultiplatform())
                .createEmrServerlessPolicy(deployment.isCreateEmrServerlessPolicy())
                .build();
    }

    /**
     * Creates an instance of this class for deployment in AWS Lambda. Maps from the definition of a jar containing
     * lambda handlers. Note that this will not be needed if the lambda is deployed as a jar.
     *
     * @param  lambdaJar the definition of the jar
     * @return           the Docker image
     */
    public static StackDockerImage lambdaImage(LambdaJar lambdaJar) {
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

    public String getDirectoryName() {
        return directoryName;
    }

    public boolean isMultiplatform() {
        return multiplatform;
    }

    public boolean isCreateEmrServerlessPolicy() {
        return createEmrServerlessPolicy;
    }

    public Optional<LambdaJar> getLambdaJar() {
        return Optional.ofNullable(lambdaJar);
    }

    @Override
    public int hashCode() {
        return Objects.hash(imageName, directoryName, multiplatform, createEmrServerlessPolicy, lambdaJar);
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
        return Objects.equals(imageName, other.imageName) && Objects.equals(directoryName, other.directoryName) && multiplatform == other.multiplatform
                && createEmrServerlessPolicy == other.createEmrServerlessPolicy && Objects.equals(lambdaJar, other.lambdaJar);
    }

    @Override
    public String toString() {
        return "StackDockerImage{imageName=" + imageName + ", directoryName=" + directoryName +
                ", isBuildx=" + multiplatform + ", createEmrServerlessPolicy=" + createEmrServerlessPolicy +
                ", lambdaJar=" + lambdaJar + "}";
    }

    /**
     * Builds Docker image details.
     */
    public static final class Builder {
        private String imageName;
        private String directoryName;
        private boolean multiplatform;
        private boolean createEmrServerlessPolicy;
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
         * Sets the name of the directory the Docker image will be held in. During a build of Sleeper a directory will
         * be created with this name that contains the Dockerfile and any other files needed to build the image.
         *
         * @param  directoryName the directory name
         * @return               this builder
         */
        public Builder directoryName(String directoryName) {
            this.directoryName = directoryName;
            return this;
        }

        /**
         * Sets whether this image should be built for multiple platforms.
         *
         * @param  multiplatform true if the image should be multiplatform
         * @return               this builder
         */
        public Builder multiplatform(boolean multiplatform) {
            this.multiplatform = multiplatform;
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
