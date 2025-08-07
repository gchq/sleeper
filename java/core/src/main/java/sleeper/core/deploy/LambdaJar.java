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

/**
 * Definitions of jar files used to deploy lambda functions.
 */
public class LambdaJar {

    private static final List<LambdaJar> ALL = new ArrayList<>();
    // The Athena plugin includes Hadoop, which makes the jar too big to deploy directly.
    // It also uses AWS SDK v1, which takes up significant space in the jar when combined with AWS SDK v2 and Hadoop.
    public static final LambdaJar ATHENA = builder().filenameFormat("athena-%s.jar")
            .imageName("athena-lambda")
            .artifactID("athena")
            .alwaysDockerDeploy(true).add();
    public static final LambdaJar BULK_IMPORT_STARTER = builder()
            .filenameFormat("bulk-import-starter-%s.jar")
            .imageName("bulk-import-starter-lambda")
            .artifactID("bulk-import-starter").add();
    public static final LambdaJar BULK_EXPORT_PLANNER = builder()
            .filenameFormat("bulk-export-planner-%s.jar")
            .imageName("bulk-export-planner")
            .artifactID("bulk-export-planner").add();
    public static final LambdaJar BULK_EXPORT_TASK_CREATOR = builder()
            .filenameFormat("bulk-export-task-creator-%s.jar")
            .imageName("bulk-export-task-creator")
            .artifactID("bulk-export-task-creator").add();
    public static final LambdaJar INGEST_TASK_CREATOR = builder()
            .filenameFormat("ingest-taskrunner-%s.jar")
            .imageName("ingest-task-creator-lambda")
            .artifactID("ingest-taskrunner").add();
    public static final LambdaJar INGEST_BATCHER_SUBMITTER = builder()
            .filenameFormat("ingest-batcher-submitter-%s.jar")
            .imageName("ingest-batcher-submitter-lambda")
            .artifactID("ingest-batcher-submitter").add();
    public static final LambdaJar INGEST_BATCHER_JOB_CREATOR = builder()
            .filenameFormat("ingest-batcher-job-creator-%s.jar")
            .imageName("ingest-batcher-job-creator-lambda")
            .artifactID("ingest-batcher-job-creator").add();
    public static final LambdaJar GARBAGE_COLLECTOR = builder()
            .filenameFormat("lambda-garbagecollector-%s.jar")
            .imageName("garbage-collector-lambda")
            .artifactID("garbage-collector").add();
    public static final LambdaJar COMPACTION_JOB_CREATOR = builder()
            .filenameFormat("lambda-jobSpecCreationLambda-%s.jar")
            .imageName("compaction-job-creator-lambda")
            .artifactID("compaction-job-creation-lambda").add();
    public static final LambdaJar COMPACTION_TASK_CREATOR = builder()
            .filenameFormat("runningjobs-%s.jar")
            .imageName("compaction-task-creator-lambda")
            .artifactID("compaction-task-creation").add();
    public static final LambdaJar PARTITION_SPLITTER = builder()
            .filenameFormat("lambda-splitter-%s.jar")
            .imageName("partition-splitter-lambda")
            .artifactID("splitter-lambda").add();

    // The query module includes Hadoop, which makes the jar too big to deploy directly.
    // It seems difficult to reduce this significantly, but this may be unnecessary if we switch to using DataFusion
    // for queries.
    public static final LambdaJar QUERY = builder().filenameFormat("query-%s.jar")
            .imageName("query-lambda")
            .artifactID("query-lambda")
            .alwaysDockerDeploy(true).add();
    public static final LambdaJar CUSTOM_RESOURCES = builder().filenameFormat("cdk-custom-resources-%s.jar")
            .imageName("custom-resources-lambda")
            .artifactID("cdk-custom-resources").add();
    public static final LambdaJar METRICS = builder().filenameFormat("metrics-%s.jar")
            .imageName("metrics-lambda")
            .artifactID("metrics").add();
    public static final LambdaJar STATESTORE = builder().filenameFormat("statestore-lambda-%s.jar")
            .imageName("statestore-lambda")
            .artifactID("statestore-lambda").add();

    private final String filename;
    private final String imageName;
    private final boolean alwaysDockerDeploy;
    private final String filenameFormat;
    private final String artifactID;

    private LambdaJar(Builder builder) {
        this.filenameFormat = requireNonNull(builder.filenameFormat, "filename must not be null");
        this.filename = String.format(filenameFormat, SleeperVersion.getVersion());
        this.imageName = requireNonNull(builder.imageName, "imageName must not be null");
        this.alwaysDockerDeploy = requireNonNull(builder.alwaysDockerDeploy, "alwaysDockerDeploy must not be null");
        this.artifactID = requireNonNull(builder.artifactID, "Artifact Id must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getFilename() {
        return filename;
    }

    public String getImageName() {
        return imageName;
    }

    public boolean isAlwaysDockerDeploy() {
        return alwaysDockerDeploy;
    }

    public String getFilenameFormat() {
        return filenameFormat;
    }

    public String getArtifactID() {
        return artifactID;
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
     * Retrieves the name of the ECR repository for deploying this jar as a Docker container.
     *
     * @param  instanceProperties the instance properties
     * @return                    the ECR repository name
     */
    public String getEcrRepositoryName(InstanceProperties instanceProperties) {
        return DockerDeployment.getEcrRepositoryPrefix(instanceProperties) + "/" + imageName;
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
                .map(LambdaJar::getFilename)
                .anyMatch(fileName::equals);
    }

    @Override
    public String toString() {
        return "LambdaJar{filename=" + filename + ", imageName=" + imageName + ", artifactId=" + artifactID + "}";
    }

    public static List<LambdaJar> getAll() {
        return Collections.unmodifiableList(ALL);
    }

    /**
     * Builder for lambda jar objects.
     */
    public static class Builder {
        private String filenameFormat;
        private String imageName;
        private String artifactID;
        private boolean alwaysDockerDeploy = false;

        private Builder() {

        }

        /**
         * Sets the filename format string.
         * String.format is used to add the version to the filename.
         *
         * @param  filenameFormat format for filename with space for version
         * @return                Builder
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
         * Sets the artifactID for the jar.
         *
         * @param  artifactID the ID of the artefact
         * @return            the builder for method chaining
         */
        public Builder artifactID(String artifactID) {
            this.artifactID = artifactID;
            return this;
        }

        /**
         * Sets the flag for if the jar is always deployed through Docker.
         *
         * @param  alwaysDockerDeploy flag for if jar is always deployed through Docker
         * @return                    builder
         */
        public Builder alwaysDockerDeploy(boolean alwaysDockerDeploy) {
            this.alwaysDockerDeploy = alwaysDockerDeploy;
            return this;
        }

        public LambdaJar build() {
            return new LambdaJar(this);
        }

        /**
         * Builds the lambda jar object and adds it to the ALL List.
         *
         * @return LambdaJar
         */
        public LambdaJar add() {
            LambdaJar lambdaJar = build();
            ALL.add(lambdaJar);
            return lambdaJar;
        }
    }
}
