/*
 * Copyright 2022-2025 Crown Copyright
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

package sleeper.core.tracker.compaction.task;

import sleeper.core.tracker.job.run.AggregatedTaskJobRuns;
import sleeper.core.tracker.job.run.JobRun;
import sleeper.core.tracker.job.run.JobRunReport;
import sleeper.core.tracker.job.status.AggregatedTaskJobsFinishedStatus;
import sleeper.core.tracker.job.status.TimeWindowQuery;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * A report of a compaction task held in the task tracker.
 */
public class CompactionTaskStatus {
    private final String taskId;
    private final Instant startTime;
    private final CompactionTaskFinishedStatus finishedStatus;
    private final Instant expiryDate; // Set by database (null before status is saved)

    private CompactionTaskStatus(Builder builder) {
        taskId = Objects.requireNonNull(builder.taskId, "taskId must not be null");
        startTime = Objects.requireNonNull(builder.startTime, "startTime must not be null");
        finishedStatus = builder.finishedStatus;
        expiryDate = builder.expiryDate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getTaskId() {
        return taskId;
    }

    public CompactionTaskFinishedStatus getFinishedStatus() {
        return finishedStatus;
    }

    /**
     * Checks if the task is contained in or overlaps with the given time period.
     *
     * @param  windowStartTime the start time
     * @param  windowEndTime   the end time
     * @return                 true if the task is in the period
     */
    public boolean isInPeriod(Instant windowStartTime, Instant windowEndTime) {
        TimeWindowQuery timeWindowQuery = new TimeWindowQuery(windowStartTime, windowEndTime);
        if (isFinished()) {
            return timeWindowQuery.isFinishedJobInWindow(startTime, finishedStatus.getFinishTime());
        } else {
            return timeWindowQuery.isUnfinishedJobInWindow(startTime);
        }
    }

    public Instant getStartTime() {
        return startTime;
    }

    /**
     * Retrieves the time the task finished, if it has finished.
     *
     * @return the time the task finished, or null if it is unfinished
     */
    public Instant getFinishTime() {
        if (isFinished()) {
            return finishedStatus.getFinishTime();
        } else {
            return null;
        }
    }

    /**
     * Retrieves the amount of time the task ran, if it has finished.
     *
     * @return the amount of time the task ran for, or null if it is unfinished
     */
    public Duration getDuration() {
        if (isFinished()) {
            return Duration.between(startTime, finishedStatus.getFinishTime());
        } else {
            return null;
        }
    }

    public Instant getExpiryDate() {
        return expiryDate;
    }

    public boolean isFinished() {
        return finishedStatus != null;
    }

    /**
     * Retrieves the number of job runs that occurred in the task, if it has finished.
     *
     * @return the number of job runs that occurred in the task, or null if it is unfinished
     */
    public Integer getJobRunsOrNull() {
        if (isFinished()) {
            return finishedStatus.getTotalJobRuns();
        } else {
            return null;
        }
    }

    /**
     * Retrieves the number of job runs that occurred in the task, if it has finished.
     *
     * @return the number of job runs that occurred in the task, or 0 if it is unfinished
     */
    public int getJobRuns() {
        if (isFinished()) {
            return finishedStatus.getTotalJobRuns();
        } else {
            return 0;
        }
    }

    /**
     * Creates a report of job runs that occurred in the task. Treats the task as though it is a job to run
     * compaction jobs. Includes aggregated statistics from the compaction job runs that occur in this task.
     *
     * @return a {@link JobRun} object
     */
    public JobRunReport asJobRunReport() {
        return AggregatedTaskJobRuns.from(taskId, CompactionTaskStartedStatus.startTime(getStartTime()), asFinishedStatus());
    }

    private AggregatedTaskJobsFinishedStatus asFinishedStatus() {
        if (finishedStatus == null) {
            return null;
        }
        return AggregatedTaskJobsFinishedStatus.updateTimeAndSummary(
                finishedStatus.getFinishTime(),
                finishedStatus.asSummary(getStartTime()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactionTaskStatus that = (CompactionTaskStatus) o;
        return taskId.equals(that.taskId)
                && startTime.equals(that.startTime)
                && Objects.equals(finishedStatus, that.finishedStatus)
                && Objects.equals(expiryDate, that.expiryDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, startTime, finishedStatus, expiryDate);
    }

    @Override
    public String toString() {
        return "CompactionTaskStatus{" +
                "taskId='" + taskId + '\'' +
                ", startTime=" + startTime +
                ", finishedStatus=" + finishedStatus +
                ", expiryDate=" + expiryDate +
                '}';
    }

    /**
     * Builder for compaction task statuses.
     */
    public static final class Builder {
        private String taskId;
        private Instant startTime;
        private CompactionTaskFinishedStatus finishedStatus;
        private Instant expiryDate;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        /**
         * Sets the task ID.
         *
         * @param  taskId the task ID
         * @return        this builder
         */
        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        /**
         * Sets the time the task started.
         *
         * @param  startTime the start time
         * @return           this builder
         */
        public Builder startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Sets the status update when the task finished. This is also used by {@link #finished}.
         *
         * @param  finishedStatus the status update
         * @return                this builder
         */
        public Builder finishedStatus(CompactionTaskFinishedStatus finishedStatus) {
            this.finishedStatus = finishedStatus;
            return this;
        }

        /**
         * Sets the expiry date, after which events for this task will no longer be held.
         *
         * @param  expiryDate the expiry date
         * @return            this builder
         */
        public Builder expiryDate(Instant expiryDate) {
            this.expiryDate = expiryDate;
            return this;
        }

        /**
         * Finishes the task and computes aggregated statistics. Sets the finished status update.
         *
         * @param  finishTime          the time the task finished
         * @param  taskFinishedBuilder the builder that was used to track job runs in the task
         * @return                     this builder
         */
        public Builder finished(Instant finishTime, CompactionTaskFinishedStatus.Builder taskFinishedBuilder) {
            return finishedStatus(taskFinishedBuilder
                    .finish(finishTime)
                    .build());
        }

        public String getTaskId() {
            return this.taskId;
        }

        public CompactionTaskStatus build() {
            return new CompactionTaskStatus(this);
        }
    }
}
