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
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Definitions of jar files used to deploy lambda functions.
 */
public class LambdaJar {

    private static final List<LambdaJar> ALL = new ArrayList<>();
    // The Athena plugin includes Hadoop, which makes the jar too big to deploy directly.
    // It also uses AWS SDK v1, which takes up significant space in the jar when combined with AWS SDK v2 and Hadoop.
    public static final LambdaJar ATHENA = withFormatAndImageDeployWithDocker("athena-%s.jar", "athena-lambda");
    public static final LambdaJar BULK_IMPORT_STARTER = withFormatAndImage("bulk-import-starter-%s.jar", "bulk-import-starter-lambda");
    public static final LambdaJar BULK_EXPORT_PLANNER = withFormatAndImage("bulk-export-planner-%s.jar", "bulk-export-planner");
    public static final LambdaJar BULK_EXPORT_TASK_CREATOR = withFormatAndImage("bulk-export-task-creator-%s.jar", "bulk-export-task-creator");
    public static final LambdaJar INGEST_TASK_CREATOR = withFormatAndImage("ingest-taskrunner-%s.jar", "ingest-task-creator-lambda");
    public static final LambdaJar INGEST_BATCHER_SUBMITTER = withFormatAndImage("ingest-batcher-submitter-%s.jar", "ingest-batcher-submitter-lambda");
    public static final LambdaJar INGEST_BATCHER_JOB_CREATOR = withFormatAndImage("ingest-batcher-job-creator-%s.jar", "ingest-batcher-job-creator-lambda");
    public static final LambdaJar GARBAGE_COLLECTOR = withFormatAndImage("lambda-garbagecollector-%s.jar", "garbage-collector-lambda");
    public static final LambdaJar COMPACTION_JOB_CREATOR = withFormatAndImage("lambda-jobSpecCreationLambda-%s.jar", "compaction-job-creator-lambda");
    public static final LambdaJar COMPACTION_TASK_CREATOR = withFormatAndImage("runningjobs-%s.jar", "compaction-task-creator-lambda");
    public static final LambdaJar PARTITION_SPLITTER = withFormatAndImage("lambda-splitter-%s.jar", "partition-splitter-lambda");
    // The query module includes Hadoop, which makes the jar too big to deploy directly.
    // It seems difficult to reduce this significantly, but this may be unnecessary if we switch to using DataFusion
    // for queries.
    public static final LambdaJar QUERY = withFormatAndImageDeployWithDocker("query-%s.jar", "query-lambda");
    public static final LambdaJar CUSTOM_RESOURCES = withFormatAndImage("cdk-custom-resources-%s.jar", "custom-resources-lambda");
    public static final LambdaJar METRICS = withFormatAndImage("metrics-%s.jar", "metrics-lambda");
    public static final LambdaJar STATESTORE = withFormatAndImage("statestore-lambda-%s.jar", "statestore-lambda");

    private final String filename;
    private final String imageName;
    private final boolean alwaysDockerDeploy;
    private final String filenameFormat;

    private LambdaJar(String filenameFormat, String version, String imageName, boolean alwaysDockerDeploy) {
        this.filenameFormat = Objects.requireNonNull(filenameFormat, "filename must not be null");
        this.filename = String.format(filenameFormat, version);
        this.imageName = Objects.requireNonNull(imageName, "imageName must not be null");
        this.alwaysDockerDeploy = Objects.requireNonNull(alwaysDockerDeploy, "alwaysDockerDeploy must not be null");
    }

    /**
     * Creates a jar definition with a filename computed by adding the Sleeper version to the given format string.
     *
     * @param  format    the format string
     * @param  imageName the name of the Docker image built from this jar
     * @return           the jar definition
     */
    public static LambdaJar withFormatAndImage(String format, String imageName) {
        LambdaJar jar = new LambdaJar(format, SleeperVersion.getVersion(), imageName, false);
        ALL.add(jar);
        return jar;
    }

    /**
     * Creates a jar definition with a filename computed by adding the Sleeper version to the given format string.
     * Lambdas using this jar will always be deployed with Docker. This should be used if the jar is too big to be
     * deployed directly, and we cannot reduce the size.
     *
     * @param  format    the format string
     * @param  imageName the name of the Docker image built from this jar
     * @return           the jar definition
     */
    public static LambdaJar withFormatAndImageDeployWithDocker(String format, String imageName) {
        LambdaJar jar = new LambdaJar(format, SleeperVersion.getVersion(), imageName, true);
        ALL.add(jar);
        return jar;
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
        return "LambdaJar{filename=" + filename + ", imageName=" + imageName + "}";
    }

    public static List<LambdaJar> getAll() {
        return Collections.unmodifiableList(ALL);
    }
}
