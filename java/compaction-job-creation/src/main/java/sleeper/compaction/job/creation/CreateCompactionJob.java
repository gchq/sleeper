/*
 * Copyright 2022 Crown Copyright
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
package sleeper.compaction.job.creation;

import sleeper.compaction.job.CompactionJob;
import sleeper.statestore.FileInfo;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CreateCompactionJob {

    private final String jobId;
    private final String partitionId;
    private final String tableName;
    private final List<FileInfo> inputFiles;
    private final CompactionOutput output;
    private final String iteratorClassName;
    private final String iteratorConfig;

    private CreateCompactionJob(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        partitionId = Objects.requireNonNull(builder.partitionId, "partitionId must not be null");
        tableName = Objects.requireNonNull(builder.tableName, "tableName must not be null");
        inputFiles = Objects.requireNonNull(builder.inputFiles, "inputFiles must not be null");
        output = Objects.requireNonNull(builder.output, "output must not be null");
        iteratorClassName = builder.iteratorClassName;
        iteratorConfig = builder.iteratorConfig;

        for (FileInfo fileInfo : inputFiles) {
            if (!partitionId.equals(fileInfo.getPartitionId())) {
                throw new IllegalArgumentException("Found file with partition which is different to the provided partition (partition = "
                        + partitionId + ", FileInfo = " + fileInfo + ")");
            }
        }
    }

    public CompactionJob buildJob() {
        CompactionJob job = new CompactionJob(tableName, jobId);
        job.setPartitionId(partitionId);
        job.setInputFiles(inputFiles.stream()
                .map(FileInfo::getFilename)
                .collect(Collectors.toList()));
        output.set(job);
        job.setIteratorClassName(iteratorClassName);
        job.setIteratorConfig(iteratorConfig);
        return job;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String jobId;
        private String partitionId;
        private String tableName;
        private List<FileInfo> inputFiles;
        private CompactionOutput output;
        private String iteratorClassName;
        private String iteratorConfig;

        private Builder() {
        }

        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder partitionId(String partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder inputFiles(List<FileInfo> inputFiles) {
            this.inputFiles = inputFiles;
            return this;
        }

        public Builder outputFilePath(String outputFilePath) {
            this.output = new CompactionOutputStandard(outputFilePath);
            return this;
        }

        public Builder splitting(Function<CompactionOutputSplitting.Builder, CompactionOutputSplitting> config) {
            output = config.apply(CompactionOutputSplitting.builder());
            return this;
        }

        public Builder iteratorClassName(String iteratorClassName) {
            this.iteratorClassName = iteratorClassName;
            return this;
        }

        public Builder iteratorConfig(String iteratorConfig) {
            this.iteratorConfig = iteratorConfig;
            return this;
        }

        public CreateCompactionJob build() {
            return new CreateCompactionJob(this);
        }
    }
}
