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
 * Stores metadata about a reference to a physical file, such as its filename, which partition it is in,
 * the number of records in this section of the file, and optionally a job id indicating which compaction
 * job is responsible for compacting it.
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
        return countApproximate == fileReference.countApproximate && onlyContainsDataForThisPartition == fileReference.onlyContainsDataForThisPartition && Objects.equals(filename, fileReference.filename) && Objects.equals(partitionId, fileReference.partitionId) && Objects.equals(numberOfRecords, fileReference.numberOfRecords) && Objects.equals(jobId, fileReference.jobId) && Objects.equals(lastStateStoreUpdateTime, fileReference.lastStateStoreUpdateTime);
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

        public Builder filename(String filename) {
            this.filename = filename;
            return this;
        }

        public Builder partitionId(String partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        public Builder numberOfRecords(Long numberOfRecords) {
            this.numberOfRecords = numberOfRecords;
            return this;
        }

        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder lastStateStoreUpdateTime(Instant lastStateStoreUpdateTime) {
            this.lastStateStoreUpdateTime = lastStateStoreUpdateTime;
            return this;
        }

        public Builder countApproximate(boolean countApproximate) {
            this.countApproximate = countApproximate;
            return this;
        }

        public Builder onlyContainsDataForThisPartition(boolean onlyContainsDataForThisPartition) {
            this.onlyContainsDataForThisPartition = onlyContainsDataForThisPartition;
            return this;
        }

        public FileReference build() {
            return new FileReference(this);
        }
    }
}
