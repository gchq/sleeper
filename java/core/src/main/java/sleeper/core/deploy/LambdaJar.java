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
import sleeper.core.properties.validation.OptionalStack;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * Definitions of jar files used to deploy lambda functions.
 */
public class LambdaJar {

    private static final List<LambdaJar> ALL = new ArrayList<>();
    public static final LambdaJar ATHENA = createWithFilenameFormat("athena-%s.jar", OptionalStack.AthenaStack);
    public static final LambdaJar BULK_IMPORT_STARTER = createWithFilenameFormat("bulk-import-starter-%s.jar", OptionalStack.BULK_IMPORT_STACKS);
    public static final LambdaJar INGEST_STARTER = createWithFilenameFormat("ingest-starter-%s.jar", OptionalStack.IngestStack);
    public static final LambdaJar INGEST_BATCHER_SUBMITTER = createWithFilenameFormat("ingest-batcher-submitter-%s.jar", OptionalStack.IngestBatcherStack);
    public static final LambdaJar INGEST_BATCHER_JOB_CREATOR = createWithFilenameFormat("ingest-batcher-job-creator-%s.jar", OptionalStack.IngestBatcherStack);
    public static final LambdaJar GARBAGE_COLLECTOR = createWithFilenameFormat("lambda-garbagecollector-%s.jar", OptionalStack.GarbageCollectorStack);
    public static final LambdaJar COMPACTION_JOB_CREATOR = createWithFilenameFormat("lambda-jobSpecCreationLambda-%s.jar", OptionalStack.CompactionStack);
    public static final LambdaJar COMPACTION_TASK_CREATOR = createWithFilenameFormat("runningjobs-%s.jar", OptionalStack.CompactionStack);
    public static final LambdaJar PARTITION_SPLITTER = createWithFilenameFormat("lambda-splitter-%s.jar", OptionalStack.PartitionSplittingStack);
    public static final LambdaJar QUERY = createWithFilenameFormat("query-%s.jar", OptionalStack.QUERY_STACKS);
    public static final LambdaJar CUSTOM_RESOURCES = createWithFilenameFormat("cdk-custom-resources-%s.jar");
    public static final LambdaJar METRICS = createWithFilenameFormat("metrics-%s.jar", OptionalStack.TableMetricsStack);
    public static final LambdaJar STATESTORE = createWithFilenameFormat("statestore-lambda-%s.jar");

    private final String fileName;
    private final List<OptionalStack> optionalStacks;

    public LambdaJar(String fileName, List<OptionalStack> optionalStacks) {
        this.fileName = fileName;
        this.optionalStacks = optionalStacks;
    }

    /**
     * Returns all lambda jar definitions.
     *
     * @return the definitions
     */
    public static List<LambdaJar> all() {
        return Collections.unmodifiableList(ALL);
    }

    private static LambdaJar createWithFilenameFormat(String format, OptionalStack... optionalStacks) {
        return createWithFilenameFormat(format, List.of(optionalStacks));
    }

    private static LambdaJar createWithFilenameFormat(String format, List<OptionalStack> optionalStacks) {
        LambdaJar jar = new LambdaJar(String.format(format, SleeperVersion.getVersion()), optionalStacks);
        ALL.add(jar);
        return jar;
    }

    public String getFileName() {
        return fileName;
    }

    public List<OptionalStack> getOptionalStacks() {
        return optionalStacks;
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
