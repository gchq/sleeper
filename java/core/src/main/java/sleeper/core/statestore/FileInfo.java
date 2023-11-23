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
package sleeper.core.statestore;

import java.time.Instant;
import java.util.Objects;

/**
 * Stores metadata about a file such as its filename, which partition it is in,
 * its status (e.g. active, ready for garbage collection), the min and max
 * values in the file, and optionally a job id indicating which compaction
 * job is responsible for compacting it.
 */
public class FileInfo {
    public enum FileStatus {
        ACTIVE, READY_FOR_GARBAGE_COLLECTION
    }

    private final String filename;
    private final String partitionId;
    private final Long numberOfRecords;
    private final FileStatus fileStatus;
    private final String jobId;
    private final Long lastStateStoreUpdateTime; // The latest time (in milliseconds since the epoch) that the status of the file was updated in the StateStore
    private boolean countApproximate;
    private boolean hasAllRecordsInFile;

    private FileInfo(Builder builder) {
        filename = builder.filename;
        partitionId = builder.partitionId;
        numberOfRecords = builder.numberOfRecords;
        fileStatus = builder.fileStatus;
        jobId = builder.jobId;
        lastStateStoreUpdateTime = builder.lastStateStoreUpdateTime;
        countApproximate = builder.countApproximate;
        hasAllRecordsInFile = builder.hasAllRecordsInFile;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getFilename() {
        return filename;
    }

    public FileStatus getFileStatus() {
        return fileStatus;
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

    public boolean isCountApproximate() {
        return countApproximate;
    }

    public boolean hasAllRecordsInFile() {
        return hasAllRecordsInFile;
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
        return countApproximate == fileInfo.countApproximate && hasAllRecordsInFile == fileInfo.hasAllRecordsInFile && Objects.equals(filename, fileInfo.filename) && Objects.equals(partitionId, fileInfo.partitionId) && Objects.equals(numberOfRecords, fileInfo.numberOfRecords) && fileStatus == fileInfo.fileStatus && Objects.equals(jobId, fileInfo.jobId) && Objects.equals(lastStateStoreUpdateTime, fileInfo.lastStateStoreUpdateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, partitionId, numberOfRecords, fileStatus, jobId, lastStateStoreUpdateTime, countApproximate, hasAllRecordsInFile);
    }

    @Override
    public String toString() {
        return "FileInfo{" +
                "filename='" + filename + '\'' +
                ", partitionId='" + partitionId + '\'' +
                ", numberOfRecords=" + numberOfRecords +
                ", fileStatus=" + fileStatus +
                ", jobId='" + jobId + '\'' +
                ", lastStateStoreUpdateTime=" + lastStateStoreUpdateTime +
                ", countApproximate=" + countApproximate +
                ", hasAllRecordsInFile=" + hasAllRecordsInFile +
                '}';
    }

    public Builder toBuilder() {
        return FileInfo.builder()
                .filename(filename)
                .partitionId(partitionId)
                .numberOfRecords(numberOfRecords)
                .fileStatus(fileStatus)
                .jobId(jobId)
                .lastStateStoreUpdateTime(lastStateStoreUpdateTime)
                .countApproximate(countApproximate)
                .hasAllRecordsInFile(hasAllRecordsInFile);
    }

    public static final class Builder {
        private String filename;
        private String partitionId;
        private Long numberOfRecords;
        private FileStatus fileStatus;
        private String jobId;
        private Long lastStateStoreUpdateTime;
        private boolean countApproximate = true;
        private boolean hasAllRecordsInFile = true;

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

        public Builder fileStatus(FileStatus fileStatus) {
            this.fileStatus = fileStatus;
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

        public Builder countApproximate(boolean countApproximate) {
            this.countApproximate = countApproximate;
            return this;
        }

        public Builder hasAllRecordsInFile(boolean hasAllRecordsInFile) {
            this.hasAllRecordsInFile = hasAllRecordsInFile;
            return this;
        }

        public FileInfo build() {
            return new FileInfo(this);
        }
    }
}
