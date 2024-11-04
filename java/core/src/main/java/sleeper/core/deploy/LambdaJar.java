/*
 * Copyright 2022-2024 Crown Copyright
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

import java.nio.file.Path;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Definitions of jar files used to deploy lambda functions.
 */
public class LambdaJar {

    public static final LambdaJar ATHENA = withFormatAndImage("athena-%s.jar", "athena-lambda");
    public static final LambdaJar BULK_IMPORT_STARTER = withFormatAndImage("bulk-import-starter-%s.jar", "bulk-import-starter-lambda");
    public static final LambdaJar INGEST_TASK_CREATOR = withFormatAndImage("ingest-starter-%s.jar", "ingest-task-creator-lambda");
    public static final LambdaJar INGEST_BATCHER_SUBMITTER = withFormatAndImage("ingest-batcher-submitter-%s.jar", "ingest-batcher-submitter-lambda");
    public static final LambdaJar INGEST_BATCHER_JOB_CREATOR = withFormatAndImage("ingest-batcher-job-creator-%s.jar", "ingest-batcher-job-creator-lambda");
    public static final LambdaJar GARBAGE_COLLECTOR = withFormatAndImage("lambda-garbagecollector-%s.jar", "garbage-collector-lambda");
    public static final LambdaJar COMPACTION_JOB_CREATOR = withFormatAndImage("lambda-jobSpecCreationLambda-%s.jar", "compaction-job-creator-lambda");
    public static final LambdaJar COMPACTION_TASK_CREATOR = withFormatAndImage("runningjobs-%s.jar", "compaction-task-creator-lambda");
    public static final LambdaJar PARTITION_SPLITTER = withFormatAndImage("lambda-splitter-%s.jar", "partition-splitter-lambda");
    public static final LambdaJar QUERY = withFormatAndImage("query-%s.jar", "query-lambda");
    public static final LambdaJar CUSTOM_RESOURCES = withFormatAndImage("cdk-custom-resources-%s.jar", "custom-resources-lambda");
    public static final LambdaJar METRICS = withFormatAndImage("metrics-%s.jar", "metrics-lambda");
    public static final LambdaJar STATESTORE = withFormatAndImage("statestore-lambda-%s.jar", "statestore-lambda");

    private final String filename;
    private final String imageName;

    private LambdaJar(String filename, String imageName) {
        this.filename = Objects.requireNonNull(filename, "filename must not be null");
        this.imageName = Objects.requireNonNull(imageName, "imageName must not be null");
    }

    /**
     * Creates a jar definition with a filename computed by adding the Sleeper version to the given format string.
     *
     * @param  format    the format string
     * @param  imageName the name of the Docker image built from this jar
     * @return           the jar definition
     */
    public static LambdaJar withFormatAndImage(String format, String imageName) {
        return new LambdaJar(String.format(format, SleeperVersion.getVersion()), imageName);
    }

    public String getFilename() {
        return filename;
    }

    public String getImageName() {
        return imageName;
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
}
