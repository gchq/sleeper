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
package sleeper.container.images;

import java.util.Objects;

/**
 * A request to transfer a container image from one repository to another.
 */
public class ContainerImageTransferRequest {
    private final String sourceImageReference;
    private final String targetImageReference;
    private final ContainerRegistryCredentials.Retriever sourceCredentialsRetriever;
    private final ContainerRegistryCredentials.Retriever targetCredentialsRetriever;

    private ContainerImageTransferRequest(Builder builder) {
        sourceImageReference = Objects.requireNonNull(builder.sourceImageReference, "sourceImageReference must not be null");
        targetImageReference = Objects.requireNonNull(builder.targetImageReference, "targetImageReference must not be null");
        sourceCredentialsRetriever = builder.sourceCredentialsRetriever;
        targetCredentialsRetriever = builder.targetCredentialsRetriever;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getSourceImageReference() {
        return sourceImageReference;
    }

    public String getTargetImageReference() {
        return targetImageReference;
    }

    public ContainerRegistryCredentials.Retriever getSourceCredentialsRetriever() {
        return sourceCredentialsRetriever;
    }

    public ContainerRegistryCredentials.Retriever getTargetCredentialsRetriever() {
        return targetCredentialsRetriever;
    }

    @Override
    public String toString() {
        return "ContainerImageTransferRequest{sourceImageReference=" + sourceImageReference + ", targetImageReference=" + targetImageReference + ", sourceCredentialsRetriever="
                + sourceCredentialsRetriever + ", targetCredentialsRetriever=" + targetCredentialsRetriever + "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceImageReference, targetImageReference, sourceCredentialsRetriever, targetCredentialsRetriever);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ContainerImageTransferRequest)) {
            return false;
        }
        ContainerImageTransferRequest other = (ContainerImageTransferRequest) obj;
        return Objects.equals(sourceImageReference, other.sourceImageReference) && Objects.equals(targetImageReference, other.targetImageReference)
                && Objects.equals(sourceCredentialsRetriever, other.sourceCredentialsRetriever) && Objects.equals(targetCredentialsRetriever, other.targetCredentialsRetriever);
    }

    /**
     * A builder to create a request to transfer a container image.
     */
    public static class Builder {
        private String sourceImageReference;
        private String targetImageReference;
        private ContainerRegistryCredentials.Retriever sourceCredentialsRetriever;
        private ContainerRegistryCredentials.Retriever targetCredentialsRetriever;

        /**
         * Sets the image reference to transfer from.
         *
         * @param  sourceImageReference the source image reference
         * @return                      this builder
         */
        public Builder sourceImageReference(String sourceImageReference) {
            this.sourceImageReference = sourceImageReference;
            return this;
        }

        /**
         * Sets the image reference to transfer to.
         *
         * @param  targetImageReference the target image reference
         * @return                      this builder
         */
        public Builder targetImageReference(String targetImageReference) {
            this.targetImageReference = targetImageReference;
            return this;
        }

        /**
         * Sets the credentials for the source registry.
         *
         * @param  sourceCredentials the credentials
         * @return                   this builder
         */
        public Builder sourceCredentials(ContainerRegistryCredentials sourceCredentials) {
            return sourceCredentialsRetriever(ContainerRegistryCredentials.Retriever.returning(sourceCredentials));
        }

        /**
         * Sets the credentials for the target registry.
         *
         * @param  targetCredentials the credentials
         * @return                   this builder
         */
        public Builder targetCredentials(ContainerRegistryCredentials targetCredentials) {
            return targetCredentialsRetriever(ContainerRegistryCredentials.Retriever.returning(targetCredentials));
        }

        /**
         * Sets the credentials retriever for the source registry.
         *
         * @param  sourceCredentialsRetriever the credentials retriever
         * @return                            this builder
         */
        public Builder sourceCredentialsRetriever(ContainerRegistryCredentials.Retriever sourceCredentialsRetriever) {
            this.sourceCredentialsRetriever = sourceCredentialsRetriever;
            return this;
        }

        /**
         * Sets the credentials retriever for the target registry.
         *
         * @param  targetCredentialsRetriever the credentials retriever
         * @return                            this builder
         */
        public Builder targetCredentialsRetriever(ContainerRegistryCredentials.Retriever targetCredentialsRetriever) {
            this.targetCredentialsRetriever = targetCredentialsRetriever;
            return this;
        }

        public ContainerImageTransferRequest build() {
            return new ContainerImageTransferRequest(this);
        }
    }
}
