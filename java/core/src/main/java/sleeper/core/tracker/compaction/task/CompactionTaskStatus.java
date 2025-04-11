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
     * This method checks wether the task was ran between a start and end time.
     * If the tass hasn't finished yet then it just checks the start time was in the period.
     *
     * @param  windowStartTime Instant for the start time of the period to check.
     * @param  windowEndTime   Instant for the end time of the period to check.
     * @return                 boolean for if the task was ran in the time period.
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
     * This method gets the finsih time for a task provided it has finished.
     *
     * @return Instant of the time task finished or null if unfinished.
     */
    public Instant getFinishTime() {
        if (isFinished()) {
            return finishedStatus.getFinishTime();
        } else {
            return null;
        }
    }

    /**
     * This method returns the Duration the task ran for provided it's finished.
     *
     * @return Duration of time bettween task start and finish or null if unfinished.
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
     * This method checks if the task has finished and if so returns the total job runs for the task.
     *
     * @return Integer for the total job runs or null if unfinished.
     */
    public Integer getJobRunsOrNull() {
        if (isFinished()) {
            return finishedStatus.getTotalJobRuns();
        } else {
            return null;
        }
    }

    /**
     * This method checks if the task has finished and if so returns the total job runs for the task.
     *
     * @return Integer for the total job runs or 0 if unfinished.
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
     * Builder class for CompactionTaskStatus.
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
         * Sets the taskId and returns the Builder.
         *
         * @param  taskId String taskId to be set.
         * @return        Builder containing current set values.
         */
        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        /**
         * Sets the startTime and retuns the Builder.
         *
         * @param  startTime Instant of the startTime to be set.
         * @return           Builder containing current set values.
         */
        public Builder startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Sets the finshed status and returns the builder.
         *
         * @param  finishedStatus CompactionTaskFinishedStatus - the specific finish status to be set.
         * @return                Builder containing current set values.
         */
        public Builder finishedStatus(CompactionTaskFinishedStatus finishedStatus) {
            this.finishedStatus = finishedStatus;
            return this;
        }

        /**
         * Sets the expiryDate and retuns the Builder.
         *
         * @param  expiryDate Instant of the expiryDate to be set.
         * @return            Builder containing current set values.
         */
        public Builder expiryDate(Instant expiryDate) {
            this.expiryDate = expiryDate;
            return this;
        }

        /**
         * Sets the task to finished using a task finished status builder and finish time.
         *
         * @param  finishTime          Instant of the time the task finished.
         * @param  taskFinishedBuilder CompactionTaskFinishedStatus Builder to be used to build the finished status.
         * @return                     Builder containing current set values.
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
