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

package sleeper.core.tracker.ingest.job.update;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * An event for when an ingest job was validated. Used in the ingest job tracker.
 */
public class IngestJobValidatedEvent implements IngestJobEvent {
    private final String jobId;
    private final String tableId;
    private final int fileCount;
    private final Instant validationTime;
    private final List<String> reasons;
    private final String jobRunId;
    private final String taskId;
    private final String jsonMessage;

    private IngestJobValidatedEvent(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        tableId = builder.tableId;
        fileCount = builder.fileCount;
        validationTime = Objects.requireNonNull(builder.validationTime, "validationTime must not be null");
        reasons = Objects.requireNonNull(builder.reasons, "reasons must not be null");
        jobRunId = builder.jobRunId;
        taskId = builder.taskId;
        jsonMessage = builder.jsonMessage;
    }

    /**
     * Creates an instance of this class for when an ingest job failed validation checks.
     *
     * @param  jobId          the ingest job ID
     * @param  jsonMessage    the JSON message
     * @param  validationTime the validation time
     * @param  reasons        the reasons why the validation failed
     * @return                an instance of this class
     */
    public static IngestJobValidatedEvent ingestJobRejected(String jobId, String jsonMessage, Instant validationTime, String... reasons) {
        return builder()
                .jobId(jobId)
                .validationTime(validationTime)
                .reasons(reasons)
                .jsonMessage(jsonMessage)
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getJobId() {
        return jobId;
    }

    public String getTableId() {
        return tableId;
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
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        IngestJobValidatedEvent that = (IngestJobValidatedEvent) object;
        return fileCount == that.fileCount && Objects.equals(jobId, that.jobId)
                && Objects.equals(tableId, that.tableId)
                && Objects.equals(validationTime, that.validationTime) && Objects.equals(reasons, that.reasons)
                && Objects.equals(jobRunId, that.jobRunId) && Objects.equals(taskId, that.taskId)
                && Objects.equals(jsonMessage, that.jsonMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, tableId, fileCount, validationTime, reasons, jobRunId, taskId, jsonMessage);
    }

    @Override
    public String toString() {
        return "IngestJobValidatedEvent{" +
                "jobId='" + jobId + '\'' +
                ", tableId='" + tableId + '\'' +
                ", fileCount=" + fileCount +
                ", validationTime=" + validationTime +
                ", reasons=" + reasons +
                ", jobRunId='" + jobRunId + '\'' +
                ", taskId='" + taskId + '\'' +
                ", jsonMessage='" + jsonMessage + '\'' +
                '}';
    }

    /**
     * Builder for ingest job validated event objects.
     */
    public static final class Builder {
        private String jobId;
        private String tableId;
        private int fileCount;
        private Instant validationTime;
        private List<String> reasons;
        private String jobRunId;
        private String taskId;
        private String jsonMessage;

        private Builder() {
        }

        /**
         * Sets the job ID.
         *
         * @param  jobId the job ID
         * @return       the builder
         */
        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        /**
         * Sets the table ID.
         *
         * @param  tableId the table ID
         * @return         the builder
         */
        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        /**
         * Sets the input file count.
         *
         * @param  fileCount the input file count
         * @return           the builder
         */
        public Builder fileCount(int fileCount) {
            this.fileCount = fileCount;
            return this;
        }

        /**
         * Sets the validation time.
         *
         * @param  validationTime the validation time
         * @return                the builder
         */
        public Builder validationTime(Instant validationTime) {
            this.validationTime = validationTime;
            return this;
        }

        /**
         * Sets the reasons why the validation failed.
         *
         * @param  reasons the list of reasons why the validation failed
         * @return         the builder
         */
        public Builder reasons(List<String> reasons) {
            this.reasons = reasons;
            return this;
        }

        /**
         * Sets the reasons why the validation failed.
         *
         * @param  reasons the reasons why the validation failed
         * @return         the builder
         */
        public Builder reasons(String... reasons) {
            return reasons(List.of(reasons));
        }

        /**
         * Sets the job run ID.
         *
         * @param  jobRunId the job run ID
         * @return          the builder
         */
        public Builder jobRunId(String jobRunId) {
            this.jobRunId = jobRunId;
            return this;
        }

        /**
         * Sets the task ID.
         *
         * @param  taskId the task ID
         * @return        the builder
         */
        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        /**
         * Sets the JSON message.
         *
         * @param  jsonMessage the JSON message
         * @return             the builder
         */
        public Builder jsonMessage(String jsonMessage) {
            this.jsonMessage = jsonMessage;
            return this;
        }

        public IngestJobValidatedEvent build() {
            return new IngestJobValidatedEvent(this);
        }
    }
}
