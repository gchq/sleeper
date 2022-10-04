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
    private String tableName;
    private final String jobId;
    private List<String> inputFiles;
    private String outputFile;
    private MutablePair<String, String> outputFiles;
    private List<String> childPartitions;
    private String partitionId;
    private final boolean isSplittingJob;
    private Object splitPoint;
    private int dimension; // Determines the row key to be used for splitting
    private String iteratorClassName;
    private String iteratorConfig;

    private CompactionJob(Builder builder) {
        setTableName(builder.tableName);
        jobId = builder.jobId;
        setInputFiles(builder.inputFiles);
        setOutputFile(builder.outputFile);
        setOutputFiles(builder.outputFiles);
        setChildPartitions(builder.childPartitions);
        setPartitionId(builder.partitionId);
        isSplittingJob = builder.isSplittingJob;
        setSplitPoint(builder.splitPoint);
        setDimension(builder.dimension);
        setIteratorClassName(builder.iteratorClassName);
        setIteratorConfig(builder.iteratorConfig);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getTableName() {
        return tableName;
    }

    private void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getId() {
        return jobId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    private void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

    public List<String> getChildPartitions() {
        return childPartitions;
    }

    private void setChildPartitions(List<String> childPartitions) {
        this.childPartitions = childPartitions;
    }

    public List<String> getInputFiles() {
        return inputFiles;
    }

    private void setInputFiles(List<String> inputFiles) {
        checkDuplicates(inputFiles);
        this.inputFiles = inputFiles;
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

    private void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }

    public Object getSplitPoint() {
        if (splitPoint instanceof ByteArray) {
            return ((ByteArray) splitPoint).getArray();
        }
        return splitPoint;
    }

    private void setSplitPoint(Object splitPoint) {
        if (splitPoint instanceof byte[]) {
            this.splitPoint = ByteArray.wrap((byte[]) splitPoint);
        } else {
            this.splitPoint = splitPoint;
        }
    }

    public int getDimension() {
        return dimension;
    }

    private void setDimension(int dimension) {
        this.dimension = dimension;
    }

    public String getIteratorClassName() {
        return iteratorClassName;
    }

    private void setIteratorClassName(String iteratorClassName) {
        this.iteratorClassName = iteratorClassName;
    }

    public String getIteratorConfig() {
        return iteratorConfig;
    }

    private void setIteratorConfig(String iteratorConfig) {
        this.iteratorConfig = iteratorConfig;
    }

    public boolean isSplittingJob() {
        return isSplittingJob;
    }

    public Pair<String, String> getOutputFiles() {
        return outputFiles;
    }

    private void setOutputFiles(MutablePair<String, String> outputFiles) {
        this.outputFiles = outputFiles;
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
            this.splitPoint = splitPoint;
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
