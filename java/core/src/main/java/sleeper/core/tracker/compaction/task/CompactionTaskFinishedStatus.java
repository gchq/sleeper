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

import sleeper.core.tracker.job.run.AverageRecordRate;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.RowsProcessed;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * A status update for when a compaction task finished. Normally this will happen when there are no more compaction jobs
 * on the queue and the task has been idle for a given period.
 */
public class CompactionTaskFinishedStatus {
    private final Instant finishTime;
    private final int totalJobRuns;
    private final Duration timeSpentOnJobs;
    private final long totalRowsRead;
    private final long totalRowsWritten;
    private final double rowsReadPerSecond;
    private final double rowsWrittenPerSecond;

    private CompactionTaskFinishedStatus(Builder builder) {
        finishTime = Objects.requireNonNull(builder.finishTime, "finishTime must not be null");
        totalJobRuns = builder.totalJobRuns;
        timeSpentOnJobs = Objects.requireNonNull(builder.timeSpentOnJobs, "timeSpentOnJobs must not be null");
        totalRowsRead = builder.totalRowsRead;
        totalRowsWritten = builder.totalRowsWritten;
        rowsReadPerSecond = builder.rowsReadPerSecond;
        rowsWrittenPerSecond = builder.rowsWrittenPerSecond;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Instant getFinishTime() {
        return finishTime;
    }

    public int getTotalJobRuns() {
        return totalJobRuns;
    }

    public Duration getTimeSpentOnJobs() {
        return timeSpentOnJobs;
    }

    public long getTotalRowsRead() {
        return totalRowsRead;
    }

    public long getTotalRowsWritten() {
        return totalRowsWritten;
    }

    public double getRowsReadPerSecond() {
        return rowsReadPerSecond;
    }

    public double getRowsWrittenPerSecond() {
        return rowsWrittenPerSecond;
    }

    /**
     * Creates a representation of this status update as though the execution of the task was a run of a job. This
     * combines the executions of all the jobs that ran in the task, and acts as a summary for the whole task, including
     * jobs that were run by the task.
     *
     * @param  startTime the time the task started
     * @return           the summary of the task and all jobs that ran on it
     */
    public JobRunSummary asSummary(Instant startTime) {
        return new JobRunSummary(
                new RowsProcessed(totalRowsRead, totalRowsWritten),
                startTime, finishTime, timeSpentOnJobs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactionTaskFinishedStatus that = (CompactionTaskFinishedStatus) o;
        return timeSpentOnJobs.equals(that.timeSpentOnJobs)
                && Double.compare(that.totalRowsRead, totalRowsRead) == 0
                && Double.compare(that.totalRowsWritten, totalRowsWritten) == 0
                && Double.compare(that.rowsReadPerSecond, rowsReadPerSecond) == 0
                && Double.compare(that.rowsWrittenPerSecond, rowsWrittenPerSecond) == 0
                && Objects.equals(finishTime, that.finishTime)
                && Objects.equals(totalJobRuns, that.totalJobRuns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(finishTime, totalJobRuns, timeSpentOnJobs,
                totalRowsRead, totalRowsWritten, rowsReadPerSecond, rowsWrittenPerSecond);
    }

    @Override
    public String toString() {
        return "CompactionTaskFinishedStatus{" +
                "finishTime=" + finishTime +
                ", totalJobs=" + totalJobRuns +
                ", timeSpentOnJobs=" + timeSpentOnJobs +
                ", totalRowsRead=" + totalRowsRead +
                ", totalRowsWritten=" + totalRowsWritten +
                ", rowsReadPerSecond=" + rowsReadPerSecond +
                ", rowsWrittenPerSecond=" + rowsWrittenPerSecond +
                '}';
    }

    /**
     * Builder for compaction task finished status updates.
     */
    public static final class Builder {
        private Instant finishTime;
        private int totalJobRuns;
        private Duration timeSpentOnJobs;
        private long totalRowsRead;
        private long totalRowsWritten;
        private double rowsReadPerSecond;
        private double rowsWrittenPerSecond;
        private final AverageRecordRate.Builder rateBuilder = AverageRecordRate.builder();

        private Builder() {
        }

        /**
         * Sets the time the compaction task finished. This will usually be set with {@link #finish}.
         *
         * @param  finishTime the finish time
         * @return            this builder
         */
        public Builder finishTime(Instant finishTime) {
            this.finishTime = finishTime;
            return this;
        }

        /**
         * Sets the total number of job runs that occurred in the task. Note that a job could be run more than once,
         * usually if it was retried or its message was delivered multiple times. This will usually be set with
         * {@link #addJobSummary} and
         * {@link #finish}.
         *
         * @param  totalJobRuns the number of job runs
         * @return              this builder
         */
        public Builder totalJobRuns(int totalJobRuns) {
            this.totalJobRuns = totalJobRuns;
            return this;
        }

        /**
         * Sets the total amount of time spent on jobs. This does not include idle time when the task was waiting
         * to receive jobs from the queue. This will usually be set with {@link #addJobSummary} and {@link #finish}.
         *
         * @param  timeSpentOnJobs the amount of time spent on jobs
         * @return                 this builder
         */
        public Builder timeSpentOnJobs(Duration timeSpentOnJobs) {
            this.timeSpentOnJobs = timeSpentOnJobs;
            return this;
        }

        /**
         * Sets the total number of rows read during the task. This is the sum of the number of rows read
         * during all jobs that ran in the task. This will usually be set with {@link #addJobSummary} and
         * {@link #finish}.
         *
         * @param  totalRowsRead the total number of rows read
         * @return               this builder
         */
        public Builder totalRowsRead(long totalRowsRead) {
            this.totalRowsRead = totalRowsRead;
            return this;
        }

        /**
         * Sets the total number of rows written during the task. This is the sum of the number of rows written
         * during all jobs that ran in the task. This will usually be set with {@link #addJobSummary} and
         * {@link #finish}.
         *
         * @param  totalRowsWritten the total number of rows written
         * @return                  this builder
         */
        public Builder totalRowsWritten(long totalRowsWritten) {
            this.totalRowsWritten = totalRowsWritten;
            return this;
        }

        /**
         * Sets the average number of rows read per second. This will usually be set with {@link #addJobSummary} and
         * {@link #finish}.
         *
         * @param  rowsReadPerSecond the average number of rows read per second
         * @return                   this builder
         */
        public Builder rowsReadPerSecond(double rowsReadPerSecond) {
            this.rowsReadPerSecond = rowsReadPerSecond;
            return this;
        }

        /**
         * Sets the average number of rows written per second. This will usually be set with {@link #addJobSummary}
         * and {@link #finish}.
         *
         * @param  rowsWrittenPerSecond the average number of rows written per second
         * @return                      this builder
         */
        public Builder rowsWrittenPerSecond(double rowsWrittenPerSecond) {
            this.rowsWrittenPerSecond = rowsWrittenPerSecond;
            return this;
        }

        /**
         * Adds a summary of a job run that finished in the task. This is used to compute aggregated statistics for the
         * task. That happens when you call {@link #finish}.
         *
         * @param  jobSummary the job run summary
         * @return            this builder
         */
        public Builder addJobSummary(JobRunSummary jobSummary) {
            rateBuilder.summary(jobSummary);
            return this;
        }

        /**
         * Adds summaries of job runs that finished in the task. This is used to compute aggregated statistics for the
         * task. That happens when you call {@link #finish}.
         *
         * @param  jobSummaries the job run summaries
         * @return              this builder
         */
        public Builder jobSummaries(Stream<JobRunSummary> jobSummaries) {
            rateBuilder.summaries(jobSummaries);
            return this;
        }

        /**
         * Sets the finish time of the task, and computes aggregated statistics of jobs that ran in the task.
         *
         * @param  finishTime the time the task finished
         * @return            this builder
         */
        public Builder finish(Instant finishTime) {
            AverageRecordRate rate = rateBuilder.finishTime(finishTime).build();
            totalJobRuns = rate.getRunCount();
            totalRowsRead = rate.getRecordsRead();
            totalRowsWritten = rate.getRecordsWritten();
            timeSpentOnJobs = rate.getTotalDuration();
            rowsReadPerSecond = rate.getRecordsReadPerSecond();
            rowsWrittenPerSecond = rate.getRecordsWrittenPerSecond();
            this.finishTime = finishTime;
            return this;
        }

        public CompactionTaskFinishedStatus build() {
            return new CompactionTaskFinishedStatus(this);
        }
    }
}
