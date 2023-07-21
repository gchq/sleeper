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
package sleeper.statestore;

import sleeper.core.key.Key;
import sleeper.core.schema.type.PrimitiveType;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Stores metadata about a file such as its filename, which partition it is in,
 * its status (e.g. active, ready for garbage collection), the min and max
 * values in the file, and optionally a job id indicating which compaction
 * job is responsible for compacting it.
 */
public class FileInfo {
    private final List<PrimitiveType> rowKeyTypes;
    private final Key minRowKey;
    private final Key maxRowKey;
    private final String filename;
    private final String partitionId;
    private final Long numberOfRecords;
    private final String jobId;
    private final Long lastStateStoreUpdateTime; // The latest time (in milliseconds since the epoch) that the status of the file was updated in the StateStore

    private FileInfo(Builder builder) {
        this.rowKeyTypes = builder.rowKeyTypes;
        this.minRowKey = builder.minRowKey;
        this.maxRowKey = builder.maxRowKey;
        this.filename = builder.filename;
        this.partitionId = builder.partitionId;
        this.numberOfRecords = builder.numberOfRecords;
        this.jobId = builder.jobId;
        this.lastStateStoreUpdateTime = builder.lastStateStoreUpdateTime;
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<PrimitiveType> getRowKeyTypes() {
        return rowKeyTypes;
    }

    public Key getMinRowKey() {
        return minRowKey;
    }

    public Key getMaxRowKey() {
        return maxRowKey;
    }

    public String getFilename() {
        return filename;
    }

    public String getJobId() {
        return jobId;
    }

    public Long getLastStateStoreUpdateTime() {
        return lastStateStoreUpdateTime;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public Long getNumberOfRecords() {
        return numberOfRecords;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileInfo fileInfo = (FileInfo) o;

        return Objects.equals(rowKeyTypes, fileInfo.rowKeyTypes) &&
                Objects.equals(filename, fileInfo.filename) &&
                Objects.equals(partitionId, fileInfo.partitionId) &&
                Objects.equals(numberOfRecords, fileInfo.numberOfRecords) &&
                Objects.equals(minRowKey, fileInfo.minRowKey) &&
                Objects.equals(maxRowKey, fileInfo.maxRowKey) &&
                Objects.equals(jobId, fileInfo.jobId) &&
                Objects.equals(lastStateStoreUpdateTime, fileInfo.lastStateStoreUpdateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowKeyTypes, filename, partitionId,
                numberOfRecords, minRowKey, maxRowKey,
                jobId, lastStateStoreUpdateTime);
    }

    @Override
    public String toString() {
        return "FileInfo{" +
                "rowKeyTypes=" + rowKeyTypes +
                ", filename='" + filename + '\'' +
                ", partition='" + partitionId + '\'' +
                ", numberOfRecords=" + numberOfRecords +
                ", minKey=" + minRowKey +
                ", maxKey=" + maxRowKey +
                ", jobId='" + jobId + '\'' +
                ", lastStateStoreUpdateTime=" + lastStateStoreUpdateTime +
                '}';
    }

    public Builder toBuilder() {
        return FileInfo.builder()
                .rowKeyTypes(rowKeyTypes)
                .minRowKey(minRowKey)
                .maxRowKey(maxRowKey)
                .filename(filename)
                .partitionId(partitionId)
                .numberOfRecords(numberOfRecords)
                .jobId(jobId)
                .lastStateStoreUpdateTime(lastStateStoreUpdateTime);
    }

    public FileLifecycleInfo toFileLifecycleInfo(FileLifecycleInfo.FileStatus fileStatus1) {
        return FileLifecycleInfo.builder()
            .filename(filename)
            .fileStatus(fileStatus1)
            .lastStateStoreUpdateTime(lastStateStoreUpdateTime)
            .build();
    }

    public static final class Builder {
        private List<PrimitiveType> rowKeyTypes;
        private Key minRowKey;
        private Key maxRowKey;
        private String filename;
        private String partitionId;
        private Long numberOfRecords;
        private String jobId;
        private Long lastStateStoreUpdateTime;

        private Builder() {
        }

        public Builder rowKeyTypes(List<PrimitiveType> rowKeyTypes) {
            this.rowKeyTypes = rowKeyTypes;
            return this;
        }

        public Builder rowKeyTypes(PrimitiveType... rowKeyTypes) {
            this.rowKeyTypes = Arrays.asList(rowKeyTypes);
            return this;
        }

        public Builder minRowKey(Key minRowKey) {
            this.minRowKey = minRowKey;
            return this;
        }

        public Builder maxRowKey(Key maxRowKey) {
            this.maxRowKey = maxRowKey;
            return this;
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

        public Builder lastStateStoreUpdateTime(Long lastStateStoreUpdateTime) {
            this.lastStateStoreUpdateTime = lastStateStoreUpdateTime;
            return this;
        }

        public Builder lastStateStoreUpdateTime(Instant lastStateStoreUpdateTime) {
            if (lastStateStoreUpdateTime == null) {
                return lastStateStoreUpdateTime((Long) null);
            } else {
                return lastStateStoreUpdateTime(lastStateStoreUpdateTime.toEpochMilli());
            }
        }

        public FileInfo build() {
            return new FileInfo(this);
        }
    }
}
