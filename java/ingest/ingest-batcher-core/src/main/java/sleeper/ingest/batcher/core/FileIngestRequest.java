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

package sleeper.ingest.batcher.core;

import java.time.Instant;
import java.util.Objects;

public class FileIngestRequest {
    private final String file;
    private final long fileSizeBytes;
    private final String tableId;
    private final Instant receivedTime;
    private final String jobId;

    private FileIngestRequest(Builder builder) {
        file = Objects.requireNonNull(builder.file, "file must not be null");
        fileSizeBytes = builder.fileSizeBytes;
        tableId = Objects.requireNonNull(builder.tableId, "tableId must not be null");
        receivedTime = Objects.requireNonNull(builder.receivedTime, "receivedTime must not be null");
        jobId = builder.jobId;
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

    public String getTableId() {
        return tableId;
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
                .tableId(tableId)
                .receivedTime(receivedTime)
                .jobId(jobId);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        FileIngestRequest that = (FileIngestRequest) object;
        return fileSizeBytes == that.fileSizeBytes
                && Objects.equals(file, that.file)
                && Objects.equals(tableId, that.tableId)
                && Objects.equals(receivedTime, that.receivedTime)
                && Objects.equals(jobId, that.jobId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(file, fileSizeBytes, tableId, receivedTime, jobId);
    }

    @Override
    public String toString() {
        return "FileIngestRequest{" +
                "file='" + file + '\'' +
                ", fileSizeBytes=" + fileSizeBytes +
                ", tableId='" + tableId + '\'' +
                ", receivedTime=" + receivedTime +
                ", jobId='" + jobId + '\'' +
                '}';
    }

    public static final class Builder {
        private String file;
        private long fileSizeBytes;
        private String tableId;
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

        public Builder tableId(String tableId) {
            this.tableId = tableId;
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
