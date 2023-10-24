/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.compaction.job;

import com.facebook.collections.ByteArray;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Objects;

/**
 * Contains the definition of a compaction job, including the id of the job,
 * a list of the input files, the output file or files, the id of the partition,
 * and whether it is a splitting job or not.
 */
public class CompactionJob {
    private final String tableName;
    private final String jobId;
    private final List<String> inputFiles;
    private final String outputFile;
    private final MutablePair<String, String> outputFiles;
    private final List<String> childPartitions;
    private final String partitionId;
    private final boolean isSplittingJob;
    private final Object splitPoint;
    private final int dimension; // Determines the row key to be used for splitting
    private final String iteratorClassName;
    private final String iteratorConfig;

    private CompactionJob(Builder builder) {
        tableName = builder.tableName;
        jobId = builder.jobId;
        inputFiles = builder.inputFiles;
        outputFile = builder.outputFile;
        outputFiles = builder.outputFiles;
        childPartitions = builder.childPartitions;
        partitionId = builder.partitionId;
        isSplittingJob = builder.isSplittingJob;
        splitPoint = builder.splitPoint;
        dimension = builder.dimension;
        iteratorClassName = builder.iteratorClassName;
        iteratorConfig = builder.iteratorConfig;
        checkDuplicates(inputFiles);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getTableName() {
        return tableName;
    }

    public String getId() {
        return jobId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public List<String> getChildPartitions() {
        return childPartitions;
    }

    public List<String> getInputFiles() {
        return inputFiles;
    }

    /**
     * Checks that there are no duplicate entries present in the list of files.
     *
     * @param <T>   generic type of list
     * @param files list of entries to check
     * @throws IllegalArgumentException if there are any duplicate entries
     */
    private static <T> void checkDuplicates(List<T> files) {
        if (files.stream()
                .distinct()
                .count() != files.size()) {
            throw new IllegalArgumentException("Duplicate entry present in file list");
        }
    }

    public String getOutputFile() {
        return outputFile;
    }

    public Object getSplitPoint() {
        if (splitPoint instanceof ByteArray) {
            return ((ByteArray) splitPoint).getArray();
        }
        return splitPoint;
    }

    public int getDimension() {
        return dimension;
    }

    public String getIteratorClassName() {
        return iteratorClassName;
    }

    public String getIteratorConfig() {
        return iteratorConfig;
    }

    public boolean isSplittingJob() {
        return isSplittingJob;
    }

    public Pair<String, String> getOutputFiles() {
        return outputFiles;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactionJob compactionJob = (CompactionJob) o;
        return isSplittingJob == compactionJob.isSplittingJob &&
                Objects.equals(splitPoint, compactionJob.splitPoint) &&
                Objects.equals(dimension, compactionJob.dimension) &&
                Objects.equals(tableName, compactionJob.tableName) &&
                Objects.equals(jobId, compactionJob.jobId) &&
                Objects.equals(inputFiles, compactionJob.inputFiles) &&
                Objects.equals(outputFile, compactionJob.outputFile) &&
                Objects.equals(outputFiles, compactionJob.outputFiles) &&
                Objects.equals(partitionId, compactionJob.partitionId) &&
                Objects.equals(iteratorClassName, compactionJob.iteratorClassName) &&
                Objects.equals(iteratorConfig, compactionJob.iteratorConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isSplittingJob,
                splitPoint,
                dimension,
                tableName,
                jobId,
                inputFiles,
                outputFile,
                outputFiles,
                partitionId,
                iteratorClassName,
                iteratorConfig);
    }

    @Override
    public String toString() {
        return "CompactionJob{" +
                "tableName='" + tableName + '\'' +
                ", jobId='" + jobId + '\'' +
                ", inputFiles=" + inputFiles +
                ", outputFile='" + outputFile + '\'' +
                ", outputFiles=" + outputFiles +
                ", partitionId='" + partitionId + '\'' +
                ", isSplittingJob=" + isSplittingJob +
                ", splitPoint=" + splitPoint +
                ", dimension=" + dimension +
                ", iteratorClassName=" + iteratorClassName +
                ", iteratorConfig=" + iteratorConfig +
                '}';
    }

    public static final class Builder {
        private String tableName;
        private String jobId;
        private List<String> inputFiles;
        private String outputFile;
        private MutablePair<String, String> outputFiles;
        private List<String> childPartitions;
        private String partitionId;
        private boolean isSplittingJob;
        private Object splitPoint;
        private int dimension;
        private String iteratorClassName;
        private String iteratorConfig;

        private Builder() {
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder inputFiles(List<String> inputFiles) {
            this.inputFiles = inputFiles;
            return this;
        }

        public Builder outputFile(String outputFile) {
            this.outputFile = outputFile;
            return this;
        }

        public Builder outputFiles(MutablePair<String, String> outputFiles) {
            this.outputFiles = outputFiles;
            return this;
        }

        public Builder childPartitions(List<String> childPartitions) {
            this.childPartitions = childPartitions;
            return this;
        }

        public Builder partitionId(String partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        public Builder isSplittingJob(boolean isSplittingJob) {
            this.isSplittingJob = isSplittingJob;
            return this;
        }

        public Builder splitPoint(Object splitPoint) {
            if (splitPoint instanceof byte[]) {
                this.splitPoint = ByteArray.wrap((byte[]) splitPoint);
            } else {
                this.splitPoint = splitPoint;
            }
            return this;
        }

        public Builder dimension(int dimension) {
            this.dimension = dimension;
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

        public CompactionJob build() {
            return new CompactionJob(this);
        }
    }
}
