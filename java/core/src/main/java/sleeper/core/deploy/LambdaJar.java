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

import sleeper.core.SleeperVersion;
import sleeper.core.properties.instance.InstanceProperties;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACCOUNT;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;

/**
 * Definitions of jar files used to deploy lambda functions.
 */
public class LambdaJar {

    private static final List<LambdaJar> ALL = new ArrayList<>();
    // The Athena plugin includes Hadoop, which makes the jar too big to deploy directly.
    // It also uses AWS SDK v1, which takes up significant space in the jar when combined with AWS SDK v2 and Hadoop.
    public static final LambdaJar ATHENA = builder().filenameFormat("athena-%s.jar")
            .imageName("athena-lambda")
            .artifactId("athena")
            .alwaysDockerDeploy(true).add();
    public static final LambdaJar BULK_IMPORT_STARTER = builder()
            .filenameFormat("bulk-import-starter-%s.jar")
            .imageName("bulk-import-starter-lambda")
            .artifactId("bulk-import-starter").add();
    public static final LambdaJar BULK_EXPORT_PLANNER = builder()
            .filenameFormat("bulk-export-planner-%s.jar")
            .imageName("bulk-export-planner")
            .artifactId("bulk-export-planner").add();
    public static final LambdaJar BULK_EXPORT_TASK_CREATOR = builder()
            .filenameFormat("bulk-export-task-creator-%s.jar")
            .imageName("bulk-export-task-creator")
            .artifactId("bulk-export-task-creator").add();
    public static final LambdaJar INGEST_TASK_CREATOR = builder()
            .filenameFormat("ingest-taskrunner-%s.jar")
            .imageName("ingest-task-creator-lambda")
            .artifactId("ingest-taskrunner").add();
    public static final LambdaJar INGEST_BATCHER_SUBMITTER = builder()
            .filenameFormat("ingest-batcher-submitter-%s.jar")
            .imageName("ingest-batcher-submitter-lambda")
            .artifactId("ingest-batcher-submitter").add();
    public static final LambdaJar INGEST_BATCHER_JOB_CREATOR = builder()
            .filenameFormat("ingest-batcher-job-creator-%s.jar")
            .imageName("ingest-batcher-job-creator-lambda")
            .artifactId("ingest-batcher-job-creator").add();
    public static final LambdaJar GARBAGE_COLLECTOR = builder()
            .filenameFormat("lambda-garbagecollector-%s.jar")
            .imageName("garbage-collector-lambda")
            .artifactId("garbage-collector").add();
    public static final LambdaJar COMPACTION_JOB_CREATOR = builder()
            .filenameFormat("lambda-jobSpecCreationLambda-%s.jar")
            .imageName("compaction-job-creator-lambda")
            .artifactId("compaction-job-creation-lambda").add();
    public static final LambdaJar COMPACTION_TASK_CREATOR = builder()
            .filenameFormat("runningjobs-%s.jar")
            .imageName("compaction-task-creator-lambda")
            .artifactId("compaction-task-creation").add();
    public static final LambdaJar PARTITION_SPLITTER = builder()
            .filenameFormat("lambda-splitter-%s.jar")
            .imageName("partition-splitter-lambda")
            .artifactId("splitter-lambda").add();

    // The query module includes Hadoop, which makes the jar too big to deploy directly.
    // It seems difficult to reduce this significantly, but this may be unnecessary if we switch to using DataFusion
    // for queries.
    public static final LambdaJar QUERY = builder().filenameFormat("query-%s.jar")
            .imageName("query-lambda")
            .artifactId("query-lambda")
            .alwaysDockerDeploy(true).add();
    public static final LambdaJar CUSTOM_RESOURCES = builder().filenameFormat("cdk-custom-resources-%s.jar")
            .imageName("custom-resources-lambda")
            .artifactId("cdk-custom-resources").add();
    public static final LambdaJar METRICS = builder().filenameFormat("metrics-%s.jar")
            .imageName("metrics-lambda")
            .artifactId("metrics").add();
    public static final LambdaJar STATESTORE = builder().filenameFormat("statestore-lambda-%s.jar")
            .imageName("statestore-lambda")
            .artifactId("statestore-lambda").add();

    private final String filenameFormat;
    private final String imageName;
    private final String artifactId;
    private final boolean alwaysDockerDeploy;

    private LambdaJar(Builder builder) {
        this.filenameFormat = requireNonNull(builder.filenameFormat, "filename must not be null");
        this.imageName = requireNonNull(builder.imageName, "imageName must not be null");
        this.artifactId = requireNonNull(builder.artifactId, "Artifact ID must not be null");
        this.alwaysDockerDeploy = requireNonNull(builder.alwaysDockerDeploy, "alwaysDockerDeploy must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Computes the filename of this jar file for a given version of Sleeper.
     *
     * @param  version the version number
     * @return         the filename
     */
    public String getFilename(String version) {
        return String.format(filenameFormat, version);
    }

    public String getFilenameFormat() {
        return filenameFormat;
    }

    public String getImageName() {
        return imageName;
    }

    public String getArtifactId() {
        return artifactId;
    }

    public boolean isAlwaysDockerDeploy() {
        return alwaysDockerDeploy;
    }

    /**
     * Formats the filename with the supplied version.
     *
     * @param  version the version of Sleeper to use in the format
     * @return         a formatted filename with the version in it
     */
    public String getFormattedFilename(String version) {
        return String.format(filenameFormat, version);
    }

    /**
     * Retrieves the Docker image name for deploying this jar as a Docker container. Includes the repository URL and the
     * tag. This method requires that CDK defined properties are set due to requiring account and region.
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
     * Retrieves the name of the ECR repository for deploying this jar as a Docker container.
     *
     * @param  instanceProperties the instance properties
     * @return                    the ECR repository name
     */
    public String getEcrRepositoryName(InstanceProperties instanceProperties) {
        return instanceProperties.get(ECR_REPOSITORY_PREFIX) + "/" + imageName;
    }

    /**
     * Checks whether a given file is one of a specified list of jars.
     *
     * @param  file the file
     * @param  jars the jars
     * @return      true if the file is one of the given jars
     */
    public static boolean isFileJar(Path file, LambdaJar... jars) {
        return isFilenameOfJar(String.valueOf(file.getFileName()), jars);
    }

    private static boolean isFilenameOfJar(String fileName, LambdaJar... jars) {
        return Stream.of(jars)
                .map(jar -> jar.getFilename(SleeperVersion.getVersion()))
                .anyMatch(fileName::equals);
    }

    @Override
    public String toString() {
        return "LambdaJar{filenameFormat=" + filenameFormat + ", imageName=" + imageName + ", artifactId=" + artifactId + ", alwaysDockerDeploy=" + alwaysDockerDeploy + "}";
    }

    /**
     * Returns all lambda jar definitions.
     *
     * @return the definitions
     */
    public static List<LambdaJar> all() {
        return Collections.unmodifiableList(ALL);
    }

    /**
     * Builder for lambda jar objects.
     */
    public static class Builder {
        private String filenameFormat;
        private String imageName;
        private String artifactId;
        private boolean alwaysDockerDeploy = false;

        private Builder() {

        }

        /**
         * Sets the filename format string.
         * String.format is used to add the version to the filename.
         *
         * @param  filenameFormat format for filename with space for version
         * @return                the builder for method chaining
         */
        public Builder filenameFormat(String filenameFormat) {
            this.filenameFormat = filenameFormat;
            return this;
        }

        /**
         * Sets the image name.
         *
         * @param  imageName the name of the image
         * @return           the builder for method chaining
         */
        public Builder imageName(String imageName) {
            this.imageName = imageName;
            return this;
        }

        /**
         * Sets the artifact ID for the jar.
         *
         * @param  artifactId the ID of the artefact
         * @return            the builder for method chaining
         */
        public Builder artifactId(String artifactId) {
            this.artifactId = artifactId;
            return this;
        }

        /**
         * Sets the flag for if the jar is always deployed through Docker.
         *
         * @param  alwaysDockerDeploy flag for if jar is always deployed through Docker
         * @return                    the builder for method chaining
         */
        public Builder alwaysDockerDeploy(boolean alwaysDockerDeploy) {
            this.alwaysDockerDeploy = alwaysDockerDeploy;
            return this;
        }

        public LambdaJar build() {
            return new LambdaJar(this);
        }

        /**
         * Builds the lambda jar object and adds it to the ALL list.
         *
         * @return the lambda jar
         */
        private LambdaJar add() {
            LambdaJar lambdaJar = build();
            ALL.add(lambdaJar);
            return lambdaJar;
        }
    }
}
