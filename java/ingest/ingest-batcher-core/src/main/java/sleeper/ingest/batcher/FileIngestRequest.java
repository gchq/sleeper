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

package sleeper.ingest.batcher;

import java.time.Instant;
import java.util.Objects;

public class FileIngestRequest {
    private final String file;
    private final long fileSizeBytes;
    private final String tableName;
    private final Instant receivedTime;
    private final String jobId;

    private FileIngestRequest(Builder builder) {
        file = Objects.requireNonNull(builder.file, "file must not be null");
        fileSizeBytes = requirePositiveFileSize(builder.fileSizeBytes, "fileSizeBytes must be positive");
        tableName = Objects.requireNonNull(builder.tableName, "tableName must not be null");
        receivedTime = Objects.requireNonNull(builder.receivedTime, "receivedTime must not be null");
        jobId = builder.jobId;
    }

    private static long requirePositiveFileSize(long bytes, String message) {
        if (bytes < 1) {
            throw new IllegalArgumentException(message);
        } else {
            return bytes;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean isAssignedToJob() {
        return jobId != null;
    }

    public String getFile() {
        return file;
    }

    public long getFileSizeBytes() {
        return fileSizeBytes;
    }

    public String getTableName() {
        return tableName;
    }

    public Instant getReceivedTime() {
        return receivedTime;
    }

    public String getJobId() {
        return jobId;
    }

    public Builder toBuilder() {
        return builder().file(file)
                .fileSizeBytes(fileSizeBytes)
                .tableName(tableName)
                .receivedTime(receivedTime)
                .jobId(jobId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FileIngestRequest that = (FileIngestRequest) o;

        if (fileSizeBytes != that.fileSizeBytes) {
            return false;
        }
        if (!file.equals(that.file)) {
            return false;
        }
        if (!tableName.equals(that.tableName)) {
            return false;
        }
        if (!receivedTime.equals(that.receivedTime)) {
            return false;
        }
        return Objects.equals(jobId, that.jobId);
    }

    @Override
    public int hashCode() {
        int result = file.hashCode();
        result = 31 * result + (int) (fileSizeBytes ^ (fileSizeBytes >>> 32));
        result = 31 * result + tableName.hashCode();
        result = 31 * result + receivedTime.hashCode();
        result = 31 * result + (jobId != null ? jobId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "FileIngestRequest{" +
                "file='" + file + '\'' +
                ", fileSizeBytes=" + fileSizeBytes +
                ", tableName='" + tableName + '\'' +
                ", receivedTime=" + receivedTime +
                ", jobId='" + jobId + '\'' +
                '}';
    }

    public static final class Builder {
        private String file;
        private long fileSizeBytes;
        private String tableName;
        private Instant receivedTime;
        private String jobId;

        private Builder() {
        }

        public Builder file(String file) {
            this.file = file;
            return this;
        }

        public Builder fileSizeBytes(long fileSizeBytes) {
            this.fileSizeBytes = fileSizeBytes;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder receivedTime(Instant receivedTime) {
            this.receivedTime = receivedTime;
            return this;
        }

        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public FileIngestRequest build() {
            return new FileIngestRequest(this);
        }
    }
}
