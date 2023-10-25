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

package sleeper.ingest.job.status;

import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class IngestJobValidatedEvent {
    private final String jobId;
    private final String tableName;
    private final int fileCount;
    private final Instant validationTime;
    private final List<String> reasons;
    private final String jobRunId;
    private final String taskId;
    private final String jsonMessage;

    private IngestJobValidatedEvent(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        tableName = builder.tableName;
        fileCount = builder.fileCount;
        validationTime = Objects.requireNonNull(builder.validationTime, "validationTime must not be null");
        reasons = Objects.requireNonNull(builder.reasons, "reasons must not be null");
        jobRunId = builder.jobRunId;
        taskId = builder.taskId;
        jsonMessage = builder.jsonMessage;
    }

    public static Builder ingestJobAccepted(IngestJob job, Instant validationTime) {
        return builder().job(job).validationTime(validationTime).reasons(List.of());
    }

    public static IngestJobValidatedEvent ingestJobRejected(String jobId, String jsonMessage, Instant validationTime, String... reasons) {
        return ingestJobRejected(jobId, jsonMessage, validationTime, List.of(reasons));
    }

    public static IngestJobValidatedEvent ingestJobRejected(String jobId, String jsonMessage, Instant validationTime, List<String> reasons) {
        return builder().job(IngestJob.builder().id(jobId).build()).validationTime(validationTime)
                .jsonMessage(jsonMessage).reasons(reasons).build();
    }

    public static IngestJobValidatedEvent ingestJobRejected(IngestJob job, Instant validationTime, String... reasons) {
        return builder().job(job).validationTime(validationTime).reasons(reasons).build();
    }

    public static IngestJobValidatedEvent ingestJobRejected(IngestJob job, Instant validationTime, List<String> reasons) {
        return builder().job(job).validationTime(validationTime).reasons(reasons).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public IngestJobValidatedStatus toStatusUpdate(Instant updateTime) {
        if (isAccepted()) {
            return IngestJobAcceptedStatus.from(
                    getFileCount(), validationTime, updateTime);
        } else {
            return IngestJobRejectedStatus.builder()
                    .inputFileCount(getFileCount())
                    .validationTime(validationTime)
                    .updateTime(updateTime)
                    .reasons(reasons)
                    .jsonMessage(jsonMessage).build();
        }
    }

    public String getJobId() {
        return jobId;
    }

    public String getTableName() {
        return tableName;
    }

    public int getFileCount() {
        return fileCount;
    }

    public String getJobRunId() {
        return jobRunId;
    }

    public String getTaskId() {
        return taskId;
    }

    public Instant getValidationTime() {
        return validationTime;
    }

    public boolean isAccepted() {
        return reasons.isEmpty();
    }

    public List<String> getReasons() {
        return reasons;
    }

    public String getJsonMessage() {
        return jsonMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IngestJobValidatedEvent that = (IngestJobValidatedEvent) o;
        return fileCount == that.fileCount && Objects.equals(jobId, that.jobId) && Objects.equals(tableName, that.tableName) && Objects.equals(validationTime, that.validationTime) && Objects.equals(reasons, that.reasons) && Objects.equals(jobRunId, that.jobRunId) && Objects.equals(taskId, that.taskId) && Objects.equals(jsonMessage, that.jsonMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, tableName, fileCount, validationTime, reasons, jobRunId, taskId, jsonMessage);
    }

    @Override
    public String toString() {
        return "IngestJobValidatedEvent{" +
                "jobId='" + jobId + '\'' +
                ", tableName='" + tableName + '\'' +
                ", fileCount=" + fileCount +
                ", validationTime=" + validationTime +
                ", reasons=" + reasons +
                ", jobRunId='" + jobRunId + '\'' +
                ", taskId='" + taskId + '\'' +
                ", jsonMessage='" + jsonMessage + '\'' +
                '}';
    }

    public static final class Builder {
        private String jobId;
        private String tableName;
        private int fileCount;
        private Instant validationTime;
        private List<String> reasons;
        private String jobRunId;
        private String taskId;
        private String jsonMessage;

        private Builder() {
        }

        public Builder job(IngestJob job) {
            return jobId(job.getId())
                    .tableName(job.getTableName())
                    .fileCount(job.getFileCount());
        }

        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder fileCount(int fileCount) {
            this.fileCount = fileCount;
            return this;
        }

        public Builder validationTime(Instant validationTime) {
            this.validationTime = validationTime;
            return this;
        }

        public Builder reasons(List<String> reasons) {
            this.reasons = reasons;
            return this;
        }

        public Builder reasons(String... reasons) {
            return reasons(List.of(reasons));
        }

        public Builder jobRunId(String jobRunId) {
            this.jobRunId = jobRunId;
            return this;
        }

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder jsonMessage(String jsonMessage) {
            this.jsonMessage = jsonMessage;
            return this;
        }

        public IngestJobValidatedEvent build() {
            return new IngestJobValidatedEvent(this);
        }
    }
}
