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

package sleeper.core.tracker.ingest.task;

import sleeper.core.tracker.job.run.JobRun;
import sleeper.core.tracker.job.status.JobRunFinishedStatus;
import sleeper.core.tracker.job.status.TimeWindowQuery;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Represents the status of an ingest task.
 */
public class IngestTaskStatus {
    private final String taskId;
    private final Instant startTime;
    private final IngestTaskFinishedStatus finishedStatus;
    private final Instant expiryDate; // Set by database (null before status is saved)

    private IngestTaskStatus(Builder builder) {
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

    public IngestTaskFinishedStatus getFinishedStatus() {
        return finishedStatus;
    }

    /**
     * Checks whether this task was run within a time window.
     *
     * @param  windowStartTime the time window start time
     * @param  windowEndTime   the time window end time
     * @return                 whether this task was run within a time window
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
     * Gets the finish time for this task.
     *
     * @return the finish time for this task, or null if the task has not finished
     */
    public Instant getFinishTime() {
        if (isFinished()) {
            return finishedStatus.getFinishTime();
        } else {
            return null;
        }
    }

    /**
     * Gets the total duration of task.
     *
     * @return the duration of this task, or null if the task has not finished
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
     * Gets the number of job runs performed by this task.
     *
     * @return the number of job runs, or null if the task has not finished
     */
    public Integer getJobRunsOrNull() {
        if (isFinished()) {
            return finishedStatus.getTotalJobRuns();
        } else {
            return null;
        }
    }

    /**
     * Gets the number of job runs performed by this task.
     *
     * @return the number of job runs, or 0 if the task has not finished
     */
    public int getJobRuns() {
        if (isFinished()) {
            return finishedStatus.getTotalJobRuns();
        } else {
            return 0;
        }
    }

    /**
     * Creates a process run object from this task.
     *
     * @return a {@link JobRun} object
     */
    public JobRun asProcessRun() {
        return JobRun.builder().taskId(taskId)
                .startedStatus(IngestTaskStartedStatus.startTime(getStartTime()))
                .finishedStatus(asProcessFinishedStatus())
                .build();
    }

    private JobRunFinishedStatus asProcessFinishedStatus() {
        if (finishedStatus == null) {
            return null;
        }
        return JobRunFinishedStatus.updateTimeAndSummary(
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
        IngestTaskStatus that = (IngestTaskStatus) o;
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
        return "IngestTaskStatus{" +
                "taskId='" + taskId + '\'' +
                ", startTime=" + startTime +
                ", finishedStatus=" + finishedStatus +
                ", expiryDate=" + expiryDate +
                '}';
    }

    /**
     * Builder for ingest task status objects.
     */
    public static final class Builder {
        private String taskId;
        private Instant startTime;
        private IngestTaskFinishedStatus finishedStatus;
        private Instant expiryDate;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        /**
         * Sets the ingest task ID.
         *
         * @param  taskId the ingest task ID
         * @return        the builder
         */
        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        /**
         * Sets the start time.
         *
         * @param  startTime the start time, in milliseconds since the epoch
         * @return           the builder
         */
        public Builder startTime(long startTime) {
            return startTime(Instant.ofEpochMilli(startTime));
        }

        /**
         * Sets the start time.
         *
         * @param  startTime the start time
         * @return           the builder
         */
        public Builder startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Sets the finished status.
         *
         * @param  finishedStatus the finished status
         * @return                the builder
         */
        public Builder finishedStatus(IngestTaskFinishedStatus finishedStatus) {
            this.finishedStatus = finishedStatus;
            return this;
        }

        /**
         * Sets the expiry date.
         *
         * @param  expiryDate the expiry date
         * @return            the builder
         */
        public Builder expiryDate(Instant expiryDate) {
            this.expiryDate = expiryDate;
            return this;
        }

        /**
         * Sets the finished status from the finish time and a task finished status builder.
         *
         * @param  finishTime          the finish time
         * @param  taskFinishedBuilder the task finished status builder
         * @return                     the builder
         */
        public Builder finished(Instant finishTime, IngestTaskFinishedStatus.Builder taskFinishedBuilder) {
            return finishedStatus(taskFinishedBuilder
                    .finish(finishTime)
                    .build());
        }

        public IngestTaskStatus build() {
            return new IngestTaskStatus(this);
        }
    }
}
