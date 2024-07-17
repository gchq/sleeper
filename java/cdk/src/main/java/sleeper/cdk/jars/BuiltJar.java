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
package sleeper.cdk.jars;

import sleeper.core.SleeperVersion;

import java.nio.file.Path;
import java.util.stream.Stream;

public class BuiltJar {

    public static final BuiltJar ATHENA = fromFormat("athena-%s.jar");
    public static final BuiltJar BULK_IMPORT_STARTER = fromFormat("bulk-import-starter-%s.jar");
    public static final BuiltJar INGEST_STARTER = fromFormat("ingest-starter-%s.jar");
    public static final BuiltJar INGEST_BATCHER_SUBMITTER = fromFormat("ingest-batcher-submitter-%s.jar");
    public static final BuiltJar INGEST_BATCHER_JOB_CREATOR = fromFormat("ingest-batcher-job-creator-%s.jar");
    public static final BuiltJar GARBAGE_COLLECTOR = fromFormat("lambda-garbagecollector-%s.jar");
    public static final BuiltJar COMPACTION_JOB_CREATOR = fromFormat("lambda-jobSpecCreationLambda-%s.jar");
    public static final BuiltJar COMPACTION_TASK_CREATOR = fromFormat("runningjobs-%s.jar");
    public static final BuiltJar PARTITION_SPLITTER = fromFormat("lambda-splitter-%s.jar");
    public static final BuiltJar QUERY = fromFormat("query-%s.jar");
    public static final BuiltJar CUSTOM_RESOURCES = fromFormat("cdk-custom-resources-%s.jar");
    public static final BuiltJar METRICS = fromFormat("metrics-%s.jar");
    public static final BuiltJar STATESTORE = fromFormat("statestore-lambda-%s.jar");

    private final String fileName;

    private BuiltJar(String fileName) {
        this.fileName = fileName;
    }

    public static BuiltJar fromFormat(String format) {
        return new BuiltJar(String.format(format, SleeperVersion.getVersion()));
    }

    public String getFileName() {
        return fileName;
    }

    public static boolean isFileJar(Path file, BuiltJar... jars) {
        return isFilenameOfJar(String.valueOf(file.getFileName()), jars);
    }

    public static boolean isFilenameOfJar(String fileName, BuiltJar... jars) {
        return Stream.of(jars)
                .map(BuiltJar::getFileName)
                .anyMatch(fileName::equals);
    }
}
