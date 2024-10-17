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
import java.util.stream.Stream;

/**
 * Definitions of jar files used to deploy lambda functions.
 */
public class LambdaJar {

    public static final LambdaJar ATHENA = createWithFilenameFormat("athena-%s.jar");
    public static final LambdaJar BULK_IMPORT_STARTER = createWithFilenameFormat("bulk-import-starter-%s.jar");
    public static final LambdaJar INGEST_STARTER = createWithFilenameFormat("ingest-starter-%s.jar");
    public static final LambdaJar INGEST_BATCHER_SUBMITTER = createWithFilenameFormat("ingest-batcher-submitter-%s.jar");
    public static final LambdaJar INGEST_BATCHER_JOB_CREATOR = createWithFilenameFormat("ingest-batcher-job-creator-%s.jar");
    public static final LambdaJar GARBAGE_COLLECTOR = createWithFilenameFormat("lambda-garbagecollector-%s.jar");
    public static final LambdaJar COMPACTION_JOB_CREATOR = createWithFilenameFormat("lambda-jobSpecCreationLambda-%s.jar");
    public static final LambdaJar COMPACTION_TASK_CREATOR = createWithFilenameFormat("runningjobs-%s.jar");
    public static final LambdaJar PARTITION_SPLITTER = createWithFilenameFormat("lambda-splitter-%s.jar");
    public static final LambdaJar QUERY = createWithFilenameFormat("query-%s.jar");
    public static final LambdaJar CUSTOM_RESOURCES = createWithFilenameFormat("cdk-custom-resources-%s.jar");
    public static final LambdaJar METRICS = createWithFilenameFormat("metrics-%s.jar");
    public static final LambdaJar STATESTORE = createWithFilenameFormat("statestore-lambda-%s.jar");

    private final String fileName;

    private LambdaJar(String fileName) {
        this.fileName = fileName;
    }

    /**
     * Creates a new jar file definition by populating a filename format with the version of Sleeper.
     *
     * @param  format the filename format
     * @return        the jar file definition
     */
    public static LambdaJar createWithFilenameFormat(String format) {
        return new LambdaJar(String.format(format, SleeperVersion.getVersion()));
    }

    public String getFileName() {
        return fileName;
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
                .map(LambdaJar::getFileName)
                .anyMatch(fileName::equals);
    }
}
