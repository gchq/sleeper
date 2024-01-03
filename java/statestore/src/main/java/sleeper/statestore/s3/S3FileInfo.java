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

package sleeper.statestore.s3;

import sleeper.core.statestore.FileInfo;

import java.util.Objects;

public class S3FileInfo {
    public enum FileStatus {
        ACTIVE, READY_FOR_GARBAGE_COLLECTION
    }

    private final FileInfo fileInfo;
    private final FileStatus status;

    private S3FileInfo(Builder builder) {
        fileInfo = builder.fileInfo;
        status = builder.status;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static S3FileInfo active(FileInfo fileInfo) {
        return builder().fileInfo(fileInfo)
                .status(FileStatus.ACTIVE).build();
    }

    public FileInfo getFileInfo() {
        return fileInfo;
    }

    public String getFilename() {
        return fileInfo.getFilename();
    }

    public String getPartitionId() {
        return fileInfo.getPartitionId();
    }

    public String getJobId() {
        return fileInfo.getJobId();
    }

    public FileStatus getFileStatus() {
        return status;
    }

    public long getLastUpdateTime() {
        return fileInfo.getLastStateStoreUpdateTime();
    }

    public S3FileInfo withUpdateTime(long updateTime) {
        return builder()
                .fileInfo(fileInfo.toBuilder().lastStateStoreUpdateTime(updateTime).build())
                .status(status)
                .build();
    }

    public S3FileInfo toReadyForGC(long updateTime) {
        return builder()
                .fileInfo(fileInfo.toBuilder().lastStateStoreUpdateTime(updateTime).build())
                .status(FileStatus.READY_FOR_GARBAGE_COLLECTION)
                .build();
    }

    public S3FileInfo withJobId(String jobId, long updateTime) {
        return builder()
                .fileInfo(fileInfo.toBuilder().jobId(jobId).lastStateStoreUpdateTime(updateTime).build())
                .status(status)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        S3FileInfo that = (S3FileInfo) o;
        return Objects.equals(fileInfo, that.fileInfo) && status == that.status;
    }

    @Override
    public String toString() {
        return "S3FileInfo{" +
                "fileInfo=" + fileInfo +
                ", status=" + status +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileInfo, status);
    }

    public static final class Builder {
        private FileInfo fileInfo;
        private FileStatus status;

        private Builder() {
        }


        public Builder fileInfo(FileInfo fileInfo) {
            this.fileInfo = fileInfo;
            return this;
        }

        public Builder status(FileStatus status) {
            this.status = status;
            return this;
        }

        public S3FileInfo build() {
            return new S3FileInfo(this);
        }
    }
}
