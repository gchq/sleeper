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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * Definitions of jar files used to deploy lambda functions.
 */
public class LambdaJar {

    private static final List<LambdaJar> ALL = new ArrayList<>();
    public static final LambdaJar ATHENA = builder()
            .fileNameFormat("athena-%s.jar")
            .imageName("athena-lambda")
            .optionalStack(OptionalStack.AthenaStack).add();
    public static final LambdaJar BULK_IMPORT_STARTER = builder()
            .fileNameFormat("bulk-import-starter-%s.jar")
            .imageName("bulk-import-starter-lambda")
            .optionalStacks(OptionalStack.BULK_IMPORT_STACKS).add();
    public static final LambdaJar INGEST_TASK_CREATOR = builder()
            .fileNameFormat("ingest-starter-%s.jar")
            .imageName("ingest-task-creator-lambda")
            .optionalStack(OptionalStack.IngestStack).add();
    public static final LambdaJar INGEST_BATCHER_SUBMITTER = builder()
            .fileNameFormat("ingest-batcher-submitter-%s.jar")
            .imageName("ingest-batcher-submitter-lambda")
            .optionalStack(OptionalStack.IngestBatcherStack).add();
    public static final LambdaJar INGEST_BATCHER_JOB_CREATOR = builder()
            .fileNameFormat("ingest-batcher-job-creator-%s.jar")
            .imageName("ingest-batcher-job-creator-lambda")
            .optionalStack(OptionalStack.IngestBatcherStack).add();
    public static final LambdaJar GARBAGE_COLLECTOR = builder()
            .fileNameFormat("lambda-garbagecollector-%s.jar")
            .imageName("garbage-collector-lambda")
            .optionalStack(OptionalStack.GarbageCollectorStack).add();
    public static final LambdaJar COMPACTION_JOB_CREATOR = builder()
            .fileNameFormat("lambda-jobSpecCreationLambda-%s.jar")
            .imageName("compaction-job-creator-lambda")
            .optionalStack(OptionalStack.CompactionStack).add();
    public static final LambdaJar COMPACTION_TASK_CREATOR = builder()
            .fileNameFormat("runningjobs-%s.jar")
            .imageName("compaction-task-creator-lambda")
            .optionalStack(OptionalStack.CompactionStack).add();
    public static final LambdaJar PARTITION_SPLITTER = builder()
            .fileNameFormat("lambda-splitter-%s.jar")
            .imageName("partition-splitter-lambda")
            .optionalStack(OptionalStack.PartitionSplittingStack).add();
    public static final LambdaJar QUERY = builder()
            .fileNameFormat("query-%s.jar")
            .imageName("query-lambda")
            .optionalStacks(OptionalStack.QUERY_STACKS).add();
    public static final LambdaJar CUSTOM_RESOURCES = builder()
            .fileNameFormat("cdk-custom-resources-%s.jar")
            .imageName("cdk-custom-resources")
            .core().add();
    public static final LambdaJar METRICS = builder()
            .fileNameFormat("metrics-%s.jar")
            .imageName("metrics-lambda")
            .optionalStack(OptionalStack.TableMetricsStack).add();
    public static final LambdaJar STATESTORE = builder()
            .fileNameFormat("statestore-lambda-%s.jar")
            .imageName("statestore-lambda")
            .core().add();

    private final String fileName;
    private final String imageName;
    private final List<OptionalStack> optionalStacks;

    private LambdaJar(Builder builder) {
        fileName = builder.fileName;
        imageName = builder.imageName;
        optionalStacks = builder.optionalStacks;
    }

    /**
     * Returns all lambda jar definitions.
     *
     * @return the definitions
     */
    public static List<LambdaJar> all() {
        return Collections.unmodifiableList(ALL);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getFileName() {
        return fileName;
    }

    public String getImageName() {
        return imageName;
    }

    public List<OptionalStack> getOptionalStacks() {
        return optionalStacks;
    }

    /**
     * Checks if this lambda is deployed given the enabled optional stacks.
     *
     * @param  stacks the enabled optional stacks
     * @return        true if this lambda will be deployed
     */
    public boolean isDeployed(Collection<OptionalStack> stacks) {
        return optionalStacks.isEmpty() || isDeployedOptional(stacks);
    }

    /**
     * Checks if this lambda is deployed in an optional stack, given the enabled optional stacks.
     *
     * @param  stacks the enabled optional stacks
     * @return        true if this lambda will be deployed in an optional stack
     */
    public boolean isDeployedOptional(Collection<OptionalStack> stacks) {
        return optionalStacks.stream().anyMatch(stacks::contains);
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

    /**
     * Builder to create a lambda jar definition.
     */
    public static class Builder {
        private String fileName;
        private String imageName;
        private List<OptionalStack> optionalStacks;

        private Builder() {
        }

        /**
         * Sets the filename by populating a format string with the Sleeper version.
         *
         * @param  format the format string
         * @return        this builder
         */
        public Builder fileNameFormat(String format) {
            this.fileName = String.format(format, SleeperVersion.getVersion());
            return this;
        }

        /**
         * Sets the Docker image name for ECR.
         *
         * @param  imageName the image name
         * @return           this builder
         */
        public Builder imageName(String imageName) {
            this.imageName = imageName;
            return this;
        }

        /**
         * Sets the optional stacks that trigger deployment of this lambda.
         *
         * @param  optionalStacks the stacks
         * @return                this builder
         */
        public Builder optionalStacks(List<OptionalStack> optionalStacks) {
            this.optionalStacks = optionalStacks;
            return this;
        }

        /**
         * Sets the optional stack that triggers deployment of this lambda.
         *
         * @param  optionalStack the stack
         * @return               this builder
         */
        public Builder optionalStack(OptionalStack optionalStack) {
            return optionalStacks(List.of(optionalStack));
        }

        /**
         * Sets that this lambda is deployed regardless of which optional stacks are enabled.
         *
         * @return this builder
         */
        public Builder core() {
            return optionalStacks(List.of());
        }

        public LambdaJar build() {
            return new LambdaJar(this);
        }

        private LambdaJar add() {
            LambdaJar jar = build();
            ALL.add(jar);
            return jar;
        }
    }
}
