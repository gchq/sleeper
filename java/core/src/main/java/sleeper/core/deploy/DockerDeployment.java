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
package sleeper.core.deploy;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.OptionalStack;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACCOUNT;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;

/**
 * A deployment of a Sleeper component that is deployed with Docker. Can be used to derive the Docker image names and
 * ECR repositories for a given Sleeper instance.
 */
public class DockerDeployment {

    private static final List<DockerDeployment> ALL = new ArrayList<>();
    public static final DockerDeployment INGEST = builder()
            .deploymentName("ingest")
            .optionalStack(OptionalStack.IngestStack)
            .add();
    public static final DockerDeployment EKS_BULK_IMPORT = builder()
            .deploymentName("bulk-import-runner")
            .optionalStack(OptionalStack.EksBulkImportStack)
            .add();
    public static final DockerDeployment COMPACTION = builder()
            .deploymentName("compaction-job-execution")
            .optionalStack(OptionalStack.CompactionStack)
            .multiplatform(true)
            .add();
    public static final DockerDeployment BULK_EXPORT = builder()
            .deploymentName("bulk-export-task-execution")
            .optionalStack(OptionalStack.BulkExportStack)
            .add();
    public static final DockerDeployment STATESTORE_COMMITTER = builder()
            .deploymentName("statestore-committer")
            .multiplatform(true)
            .add();

    private final String deploymentName;
    private final OptionalStack optionalStack;
    private final boolean multiplatform;
    private final boolean createEmrServerlessPolicy;

    private DockerDeployment(Builder builder) {
        this.deploymentName = builder.deploymentName;
        this.optionalStack = builder.optionalStack;
        this.multiplatform = builder.multiplatform;
        this.createEmrServerlessPolicy = builder.createEmrServerlessPolicy;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Retrieves all Docker deployments.
     *
     * @return the deployments
     */
    public static List<DockerDeployment> all() {
        return Collections.unmodifiableList(ALL);
    }

    /**
     * Retrieves the name of this deployment. Used as part of Docker image names and ECR repository names.
     *
     * @return the deployment name
     */
    public String getDeploymentName() {
        return deploymentName;
    }

    /**
     * Retrieves which optional stack uses this deployment. If the optional stack is not enabled, this Docker image is
     * not needed.
     *
     * @return the optional stack
     */
    public OptionalStack getOptionalStack() {
        return optionalStack;
    }

    /**
     * Checks whether the Docker image should be built for multiple platforms.
     *
     * @return true if the image is multiplatform
     */
    public boolean isMultiplatform() {
        return multiplatform;
    }

    /**
     * Checks whether the ECR repository needs a policy to let EMR Serverless pull the Docker image.
     *
     * @return true if the EMR Serverless policy is needed
     */
    public boolean isCreateEmrServerlessPolicy() {
        return createEmrServerlessPolicy;
    }

    /**
     * Retrieves the Docker image name for this deployment. Includes the repository URL and the tag.
     * This method requires that CDK defined properties are set due to requiring account and region.
     *
     * @param  properties the instance properties
     * @return            the Docker image name
     */
    public String getDockerImageName(InstanceProperties properties) {
        return properties.get(ACCOUNT) + ".dkr.ecr." +
                properties.get(REGION) + ".amazonaws.com/" +
                getEcrRepositoryName(properties) +
                ":" + properties.get(VERSION);
    }

    /**
     * Retrieves the name of the ECR repository for this docker deployment.
     *
     * @param  instanceProperties the instance properties
     * @return                    the ECR repository name
     */
    public String getEcrRepositoryName(InstanceProperties instanceProperties) {
        return instanceProperties.get(ECR_REPOSITORY_PREFIX) + "/" + deploymentName;
    }

    /**
     * Creates a Docker deployment.
     */
    public static class Builder {
        private String deploymentName;
        private OptionalStack optionalStack;
        private boolean multiplatform;
        private boolean createEmrServerlessPolicy;

        /**
         * Sets the name of this deployment. Used as part of Docker image names and ECR repository names.
         *
         * @param  deploymentName the deployment name
         * @return                this builder
         */
        public Builder deploymentName(String deploymentName) {
            this.deploymentName = deploymentName;
            return this;
        }

        /**
         * Sets which optional stack uses this deployment. If the optional stack is not enabled, this Docker deployment
         * will not be used.
         *
         * @param  optionalStack the optional stack
         * @return               this builder
         */
        public Builder optionalStack(OptionalStack optionalStack) {
            this.optionalStack = optionalStack;
            return this;
        }

        /**
         * Sets whether the Docker image should be built for multiple platforms.
         *
         * @param  multiplatform true if the image is multiplatform
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

        public DockerDeployment build() {
            return new DockerDeployment(this);
        }

        private DockerDeployment add() {
            DockerDeployment deployment = build();
            ALL.add(deployment);
            return deployment;
        }
    }
}
