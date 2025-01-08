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

import sleeper.core.tracker.job.ProcessRunTime;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * An event for when an ingest job failed. Used in the ingest job tracker.
 */
public class IngestJobFailedEvent implements IngestJobEvent {
    private final String jobId;
    private final String tableId;
    private final String jobRunId;
    private final String taskId;
    private final ProcessRunTime runTime;
    private final List<String> failureReasons;

    private IngestJobFailedEvent(Builder builder) {
        this.jobId = builder.jobId;
        this.tableId = builder.tableId;
        this.jobRunId = builder.jobRunId;
        this.taskId = builder.taskId;
        this.runTime = builder.runTime;
        this.failureReasons = builder.failureReasons;
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

    public String getJobRunId() {
        return jobRunId;
    }

    public String getTaskId() {
        return taskId;
    }

    public ProcessRunTime getRunTime() {
        return runTime;
    }

    public List<String> getFailureReasons() {
        return failureReasons;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, tableId, jobRunId, taskId, runTime);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof IngestJobFailedEvent)) {
            return false;
        }
        IngestJobFailedEvent other = (IngestJobFailedEvent) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(tableId, other.tableId) && Objects.equals(jobRunId, other.jobRunId) && Objects.equals(taskId, other.taskId)
                && Objects.equals(runTime, other.runTime);
    }

    @Override
    public String toString() {
        return "IngestJobFailedEvent{jobId=" + jobId + ", tableId=" + tableId + ", jobRunId=" + jobRunId + ", taskId=" + taskId + ", runTime=" + runTime + "}";
    }

    /**
     * Builder for ingest job failed event objects.
     */
    public static final class Builder {
        private String jobId;
        private String tableId;
        private String jobRunId;
        private String taskId;
        private ProcessRunTime runTime;
        private List<String> failureReasons;

        /**
         * Sets the ingest job ID.
         *
         * @param  jobId the ingest job ID
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
         * Sets the run time.
         *
         * @param  runTime the run time
         * @return         the builder
         */
        public Builder runTime(ProcessRunTime runTime) {
            this.runTime = runTime;
            return this;
        }

        /**
         * Sets the failure reasons.
         *
         * @param  failureReasons the failure reasons
         * @return                the builder
         */
        public Builder failureReasons(List<String> failureReasons) {
            this.failureReasons = failureReasons;
            return this;
        }

        /**
         * Sets the failure reasons from the given exception.
         *
         * @param  failure the failure
         * @return         the builder
         */
        public Builder failure(Exception failure) {
            return failureReasons(getFailureReasons(failure));
        }

        public IngestJobFailedEvent build() {
            return new IngestJobFailedEvent(this);
        }

    }

    private static List<String> getFailureReasons(Exception e) {
        List<String> reasons = new ArrayList<>();
        Throwable failure = e;
        while (failure != null) {
            reasons.add(failure.getMessage());
            failure = failure.getCause();
        }
        return reasons;
    }
}
