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
package sleeper.core.statestore;

import java.time.Instant;
import java.util.Objects;

/**
 * Stores metadata about a reference to a physical file. This includes its filename, which partition it is in,
 * the number of records in this section of the file, and optionally a job id indicating which compaction
 * job is responsible for compacting it. If a file is referenced in multiple partitions, the ranges covered by those
 * partitions must not overlap, or the records in the overlapping portion may be duplicated.
 */
public class FileReference {

    private final String filename;
    private final String partitionId;
    private final Long numberOfRecords;
    private final String jobId;
    private final Instant lastStateStoreUpdateTime;
    private final boolean countApproximate;
    private final boolean onlyContainsDataForThisPartition;

    private FileReference(Builder builder) {
        filename = Objects.requireNonNull(builder.filename, "filename must not be null");
        partitionId = Objects.requireNonNull(builder.partitionId, "partitionId must not be null");
        numberOfRecords = Objects.requireNonNull(builder.numberOfRecords, "numberOfRecords must not be null");
        jobId = builder.jobId;
        lastStateStoreUpdateTime = builder.lastStateStoreUpdateTime;
        countApproximate = builder.countApproximate;
        onlyContainsDataForThisPartition = builder.onlyContainsDataForThisPartition;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getFilename() {
        return filename;
    }

    public String getJobId() {
        return jobId;
    }

    public Instant getLastStateStoreUpdateTime() {
        return lastStateStoreUpdateTime;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public Long getNumberOfRecords() {
        return numberOfRecords;
    }

    public boolean isCountApproximate() {
        return countApproximate;
    }

    /**
     * Returns true if the file that's referenced does not contain data for any other partition.
     *
     * @return true if the file only contains data for this partition
     */
    public boolean onlyContainsDataForThisPartition() {
        return onlyContainsDataForThisPartition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileReference fileReference = (FileReference) o;
        return countApproximate == fileReference.countApproximate && onlyContainsDataForThisPartition == fileReference.onlyContainsDataForThisPartition
                && Objects.equals(filename, fileReference.filename) && Objects.equals(partitionId, fileReference.partitionId) && Objects.equals(numberOfRecords, fileReference.numberOfRecords)
                && Objects.equals(jobId, fileReference.jobId) && Objects.equals(lastStateStoreUpdateTime, fileReference.lastStateStoreUpdateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, partitionId, numberOfRecords, jobId, lastStateStoreUpdateTime, countApproximate, onlyContainsDataForThisPartition);
    }

    @Override
    public String toString() {
        return "FileReference{" +
                "filename='" + filename + '\'' +
                ", partitionId='" + partitionId + '\'' +
                ", numberOfRecords=" + numberOfRecords +
                ", jobId='" + jobId + '\'' +
                ", lastStateStoreUpdateTime=" + lastStateStoreUpdateTime +
                ", countApproximate=" + countApproximate +
                ", onlyContainsDataForThisPartition=" + onlyContainsDataForThisPartition +
                '}';
    }

    public Builder toBuilder() {
        return new Builder()
                .filename(filename)
                .partitionId(partitionId)
                .numberOfRecords(numberOfRecords)
                .jobId(jobId)
                .lastStateStoreUpdateTime(lastStateStoreUpdateTime)
                .countApproximate(countApproximate)
                .onlyContainsDataForThisPartition(onlyContainsDataForThisPartition);
    }

    /**
     * Builder to create a file reference.
     */
    public static final class Builder {
        private String filename;
        private String partitionId;
        private Long numberOfRecords;
        private String jobId;
        private Instant lastStateStoreUpdateTime;
        private boolean countApproximate;
        private boolean onlyContainsDataForThisPartition;

        private Builder() {
        }

        /**
         * Sets the filename.
         *
         * @param  filename the filename
         * @return          the builder
         */
        public Builder filename(String filename) {
            this.filename = filename;
            return this;
        }

        /**
         * Sets the ID of the partition the file is referenced on.
         *
         * @param  partitionId the partition ID
         * @return             the builder
         */
        public Builder partitionId(String partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        /**
         * Sets the number of records in the file that are in the given partition. This may be exact or an estimate.
         * This must be set alongside {@link #countApproximate(boolean)}.
         *
         * @param  numberOfRecords the number of records
         * @return                 the builder
         */
        public Builder numberOfRecords(Long numberOfRecords) {
            this.numberOfRecords = numberOfRecords;
            return this;
        }

        /**
         * Sets which job the file reference is assigned to. This will only be set if the file's records on this
         * partition are due to be processed by a job, e.g. a compaction.
         *
         * @param  jobId the ID of the job
         * @return       the builder
         */
        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        /**
         * Sets when this reference was last updated in the state store. This is independent of the last update time of
         * the referenced file, although the file will always be updated when any of its references are updated. This
         * should only be set by an implementation of the state store.
         *
         * @param  lastStateStoreUpdateTime the update time (should be set by the state store implementation)
         * @return                          the builder
         */
        public Builder lastStateStoreUpdateTime(Instant lastStateStoreUpdateTime) {
            this.lastStateStoreUpdateTime = lastStateStoreUpdateTime;
            return this;
        }

        /**
         * Sets whether the count of records in the given partition is exact or an estimate. This must be set alongside
         * {@link #numberOfRecords(Long)}.
         *
         * @param  countApproximate true if the number of records is approximate
         * @return                  the builder
         */
        public Builder countApproximate(boolean countApproximate) {
            this.countApproximate = countApproximate;
            return this;
        }

        /**
         * Sets whether this reference includes all the data in the file. If true, there are no other references to the
         * file on any other partition. If false, the file has been split into references on multiple partitions.
         *
         * @param  onlyContainsDataForThisPartition true if this is the only internal reference to the file
         * @return                                  the builder
         */
        public Builder onlyContainsDataForThisPartition(boolean onlyContainsDataForThisPartition) {
            this.onlyContainsDataForThisPartition = onlyContainsDataForThisPartition;
            return this;
        }

        public FileReference build() {
            return new FileReference(this);
        }
    }
}
