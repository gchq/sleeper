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
package sleeper.compaction.core.job.status;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.core.record.process.ProcessRunTime;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * An event for when a compaction job failed. Used in the compaction job status store.
 */
public class CompactionJobFailedEvent {
    private final String jobId;
    private final String tableId;
    private final String taskId;
    private final String jobRunId;
    private final ProcessRunTime runTime;
    private final List<String> failureReasons;

    private CompactionJobFailedEvent(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        tableId = Objects.requireNonNull(builder.tableId, "tableId must not be null");
        taskId = Objects.requireNonNull(builder.taskId, "taskId must not be null");
        jobRunId = builder.jobRunId;
        runTime = Objects.requireNonNull(builder.runTime, "runTime must not be null");
        failureReasons = Objects.requireNonNull(builder.failureReasons, "failureReasons must not be null");
    }

    /**
     * Creates a builder for this class.
     *
     * @param  job     the compaction job
     * @param  runTime the process run time
     * @return         a builder
     */
    public static Builder compactionJobFailed(CompactionJob job, ProcessRunTime runTime) {
        return builder().job(job).runTime(runTime);
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

    public String getTaskId() {
        return taskId;
    }

    public String getJobRunId() {
        return jobRunId;
    }

    public ProcessRunTime getRunTime() {
        return runTime;
    }

    public List<String> getFailureReasons() {
        return failureReasons;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, tableId, taskId, jobRunId, runTime, failureReasons);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CompactionJobFailedEvent)) {
            return false;
        }
        CompactionJobFailedEvent other = (CompactionJobFailedEvent) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(tableId, other.tableId) && Objects.equals(taskId, other.taskId) && Objects.equals(jobRunId, other.jobRunId)
                && Objects.equals(runTime, other.runTime) && Objects.equals(failureReasons, other.failureReasons);
    }

    @Override
    public String toString() {
        return "CompactionJobFailedEvent{jobId=" + jobId + ", tableId=" + tableId + ", taskId=" + taskId + ", jobRunId=" + jobRunId + ", runTime=" + runTime + ", failureReasons=" + failureReasons
                + "}";
    }

    /**
     * Builder for compaction job failed events.
     */
    public static final class Builder {
        private String jobId;
        private String tableId;
        private String jobRunId;
        private String taskId;
        private ProcessRunTime runTime;
        private List<String> failureReasons;

        /**
         * Sets the compaction job ID and the table ID using the provided compaction job.
         *
         * @param  job the compaction job
         * @return     the builder
         */
        public Builder job(CompactionJob job) {
            return jobId(job.getId())
                    .tableId(job.getTableId());
        }

        /**
         * Sets the compaction job ID.
         *
         * @param  jobId the compaction job ID
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

        public CompactionJobFailedEvent build() {
            return new CompactionJobFailedEvent(this);
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
