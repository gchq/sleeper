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

import sleeper.core.deploy.LambdaJar;

import java.util.Objects;
import java.util.Optional;

/**
 * This class is used to store details about a DockerImage that will be deployed to the stack.
 */
public class StackDockerImage {
    private final String imageName;
    private final String directoryName;
    private final boolean isBuildx;
    private final boolean createEmrServerlessPolicy;
    private final LambdaJar lambdaJar;

    private StackDockerImage(Builder builder) {
        imageName = builder.imageName;
        directoryName = builder.directoryName;
        isBuildx = builder.isBuildx;
        createEmrServerlessPolicy = builder.createEmrServerlessPolicy;
        lambdaJar = builder.lambdaJar;
    }

    /**
     * This method builds a StackDockerImage using the provided string as imagename and directoryName.
     *
     * @param  imageName String to be used for imageName and diectoryName.
     * @return           The built StackDockerImage.
     */
    public static StackDockerImage dockerBuildImage(String imageName) {
        return builder().imageName(imageName)
                .directoryName(imageName).build();
    }

    /**
     * This method builds a StackDockerImage using the provided string as imagename and directoryName with isBuildX set
     * to true.
     *
     * @param  imageName String to be used for imageName and diectoryName.
     * @return           The built StackDockerImage.
     */
    public static StackDockerImage dockerBuildxImage(String imageName) {
        return builder().imageName(imageName)
                .directoryName(imageName).isBuildx(true).build();
    }

    /**
     * This method builds a StackDockerImage using the provided string as imagename and directoryName with
     * createdEmrServerlessPolicy set to true.
     *
     * @param  imageName String to be used for imageName and diectoryName.
     * @return           The built StackDockerImage.
     */
    public static StackDockerImage emrServerlessImage(String imageName) {
        return builder().imageName(imageName)
                .directoryName(imageName).createEmrServerlessPolicy(true).build();
    }

    /**
     * This method builds a StackDockerImage from a LambdaJar.
     *
     * @param  lambdaJar The LambdarJar to be used to get the image name and lambdaJar values of the Image.
     * @return           A built StackDockerImage using the lambdarJar as imageName, 'lambda' as directory name and
     *                   lambdarJar value assigned.
     */
    public static StackDockerImage lambdaImage(LambdaJar lambdaJar) {
        return builder()
                .imageName(lambdaJar.getImageName())
                .directoryName("lambda")
                .lambdaJar(lambdaJar)
                .build();
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

    public boolean isBuildx() {
        return isBuildx;
    }

    public boolean isCreateEmrServerlessPolicy() {
        return createEmrServerlessPolicy;
    }

    public Optional<LambdaJar> getLambdaJar() {
        return Optional.ofNullable(lambdaJar);
    }

    @Override
    public int hashCode() {
        return Objects.hash(imageName, directoryName, isBuildx, createEmrServerlessPolicy, lambdaJar);
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
        return Objects.equals(imageName, other.imageName) && Objects.equals(directoryName, other.directoryName) && isBuildx == other.isBuildx
                && createEmrServerlessPolicy == other.createEmrServerlessPolicy && Objects.equals(lambdaJar, other.lambdaJar);
    }

    @Override
    public String toString() {
        return "StackDockerImage{imageName=" + imageName + ", directoryName=" + directoryName +
                ", isBuildx=" + isBuildx + ", createEmrServerlessPolicy=" + createEmrServerlessPolicy +
                ", lambdaJar=" + lambdaJar + "}";
    }

    /**
     * This is a builder class for the StackDockerImage class.
     */
    public static final class Builder {
        private String imageName;
        private String directoryName;
        private boolean isBuildx;
        private boolean createEmrServerlessPolicy;
        private LambdaJar lambdaJar;

        private Builder() {
        }

        /**
         * Sets the imageName in the builder.
         *
         * @param  imageName String image name to be set.
         * @return           This builder class with the imageName set.
         */
        public Builder imageName(String imageName) {
            this.imageName = imageName;
            return this;
        }

        /**
         * Sets the directoryName in the builder.
         *
         * @param  directoryName String directory name to be set.
         * @return               This builder class with the directoryName set.
         */
        public Builder directoryName(String directoryName) {
            this.directoryName = directoryName;
            return this;
        }

        /**
         * Sets the isBuildx in the builder.
         *
         * @param  isBuildx boolean isBuildx to be set.
         * @return          This builder class with the isBuildx set.
         */
        public Builder isBuildx(boolean isBuildx) {
            this.isBuildx = isBuildx;
            return this;
        }

        /**
         * Sets the createEmrServerlessPolicy in the builder.
         *
         * @param  createEmrServerlessPolicy boolean createEmrServerlessPolicy to be set.
         * @return                           This builder class with the createEmrServerlessPolicy set.
         */
        public Builder createEmrServerlessPolicy(boolean createEmrServerlessPolicy) {
            this.createEmrServerlessPolicy = createEmrServerlessPolicy;
            return this;
        }

        /**
         * Sets the lambdaJar in the builder.
         *
         * @param  lambdaJar boolean lambdaJar to be set.
         * @return           This builder class with the lambdaJar set.
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
