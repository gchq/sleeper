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

import java.time.Instant;
import java.util.Objects;

public class FileLifecycleInfo {
    public enum FileStatus {
        ACTIVE,
        GARBAGE_COLLECTION_PENDING
    }

    private final String filename;
    private final FileStatus fileStatus;
    private final Long lastStateStoreUpdateTime; // The latest time (in milliseconds since the epoch) that the status of the file was updated in the StateStore

    private FileLifecycleInfo(Builder builder) {
        this.filename = builder.filename;
        this.fileStatus = builder.fileStatus;
        this.lastStateStoreUpdateTime = builder.lastStateStoreUpdateTime;
    }

    public String getFilename() {
        return filename;
    }

    public FileStatus getFileStatus() {
        return fileStatus;
    }

    public Long getLastStateStoreUpdateTime() {
        return lastStateStoreUpdateTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileLifecycleInfo fileInfo = (FileLifecycleInfo) o;

        return Objects.equals(filename, fileInfo.filename) &&
                fileStatus == fileInfo.fileStatus &&
                Objects.equals(lastStateStoreUpdateTime, fileInfo.lastStateStoreUpdateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, fileStatus, lastStateStoreUpdateTime);
    }

    @Override
    public String toString() {
        return "FileLifecycleInfo{" +
                "filename='" + filename + '\'' +
                ", fileStatus=" + fileStatus +
                ", lastStateStoreUpdateTime=" + lastStateStoreUpdateTime +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public FileLifecycleInfo cloneWithStatus(FileStatus fileStatus) {
        return builder()
            .filename(getFilename())
            .fileStatus(fileStatus)
            .lastStateStoreUpdateTime(getLastStateStoreUpdateTime())
            .build();
    }

    public Builder toBuilder() {
        return FileLifecycleInfo.builder()
                .filename(filename)
                .fileStatus(fileStatus)
                .lastStateStoreUpdateTime(lastStateStoreUpdateTime);
    }

    public static final class Builder {
        private String filename;
        private FileStatus fileStatus;
        private Long lastStateStoreUpdateTime;

        private Builder() {
        }

        public Builder filename(String filename) {
            this.filename = filename;
            return this;
        }

        public Builder fileStatus(FileStatus fileStatus) {
            this.fileStatus = fileStatus;
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

        public FileLifecycleInfo build() {
            return new FileLifecycleInfo(this);
        }
    }
}
