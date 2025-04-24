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
import sleeper.core.tracker.job.run.RecordsProcessed;

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
    private final long totalRecordsRead;
    private final long totalRecordsWritten;
    private final double recordsReadPerSecond;
    private final double recordsWrittenPerSecond;

    private CompactionTaskFinishedStatus(Builder builder) {
        finishTime = Objects.requireNonNull(builder.finishTime, "finishTime must not be null");
        totalJobRuns = builder.totalJobRuns;
        timeSpentOnJobs = Objects.requireNonNull(builder.timeSpentOnJobs, "timeSpentOnJobs must not be null");
        totalRecordsRead = builder.totalRecordsRead;
        totalRecordsWritten = builder.totalRecordsWritten;
        recordsReadPerSecond = builder.recordsReadPerSecond;
        recordsWrittenPerSecond = builder.recordsWrittenPerSecond;
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

    public long getTotalRecordsRead() {
        return totalRecordsRead;
    }

    public long getTotalRecordsWritten() {
        return totalRecordsWritten;
    }

    public double getRecordsReadPerSecond() {
        return recordsReadPerSecond;
    }

    public double getRecordsWrittenPerSecond() {
        return recordsWrittenPerSecond;
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
                new RecordsProcessed(totalRecordsRead, totalRecordsWritten),
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
                && Double.compare(that.totalRecordsRead, totalRecordsRead) == 0
                && Double.compare(that.totalRecordsWritten, totalRecordsWritten) == 0
                && Double.compare(that.recordsReadPerSecond, recordsReadPerSecond) == 0
                && Double.compare(that.recordsWrittenPerSecond, recordsWrittenPerSecond) == 0
                && Objects.equals(finishTime, that.finishTime)
                && Objects.equals(totalJobRuns, that.totalJobRuns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(finishTime, totalJobRuns, timeSpentOnJobs,
                totalRecordsRead, totalRecordsWritten, recordsReadPerSecond, recordsWrittenPerSecond);
    }

    @Override
    public String toString() {
        return "CompactionTaskFinishedStatus{" +
                "finishTime=" + finishTime +
                ", totalJobs=" + totalJobRuns +
                ", timeSpentOnJobs=" + timeSpentOnJobs +
                ", totalRecordsRead=" + totalRecordsRead +
                ", totalRecordsWritten=" + totalRecordsWritten +
                ", recordsReadPerSecond=" + recordsReadPerSecond +
                ", recordsWrittenPerSecond=" + recordsWrittenPerSecond +
                '}';
    }

    /**
     * Builder for compaction task finished status updates.
     */
    public static final class Builder {
        private Instant finishTime;
        private int totalJobRuns;
        private Duration timeSpentOnJobs;
        private long totalRecordsRead;
        private long totalRecordsWritten;
        private double recordsReadPerSecond;
        private double recordsWrittenPerSecond;
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
         * Sets the total number of records read during the task. This is the sum of the number of records read
         * during all jobs that ran in the task. This will usually be set with {@link #addJobSummary} and
         * {@link #finish}.
         *
         * @param  totalRecordsRead the total number of records read
         * @return                  this builder
         */
        public Builder totalRecordsRead(long totalRecordsRead) {
            this.totalRecordsRead = totalRecordsRead;
            return this;
        }

        /**
         * Sets the total number of records written during the task. This is the sum of the number of records written
         * during all jobs that ran in the task. This will usually be set with {@link #addJobSummary} and
         * {@link #finish}.
         *
         * @param  totalRecordsWritten the total number of records written
         * @return                     this builder
         */
        public Builder totalRecordsWritten(long totalRecordsWritten) {
            this.totalRecordsWritten = totalRecordsWritten;
            return this;
        }

        /**
         * Sets the average number of records read per second. This will usually be set with {@link #addJobSummary} and
         * {@link #finish}.
         *
         * @param  recordsReadPerSecond the average number of records read per second
         * @return                      this builder
         */
        public Builder recordsReadPerSecond(double recordsReadPerSecond) {
            this.recordsReadPerSecond = recordsReadPerSecond;
            return this;
        }

        /**
         * Sets the average number of records written per second. This will usually be set with {@link #addJobSummary}
         * and {@link #finish}.
         *
         * @param  recordsWrittenPerSecond the average number of records written per second
         * @return                         this builder
         */
        public Builder recordsWrittenPerSecond(double recordsWrittenPerSecond) {
            this.recordsWrittenPerSecond = recordsWrittenPerSecond;
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
            totalRecordsRead = rate.getRecordsRead();
            totalRecordsWritten = rate.getRecordsWritten();
            timeSpentOnJobs = rate.getTotalDuration();
            recordsReadPerSecond = rate.getRecordsReadPerSecond();
            recordsWrittenPerSecond = rate.getRecordsWrittenPerSecond();
            this.finishTime = finishTime;
            return this;
        }

        public CompactionTaskFinishedStatus build() {
            return new CompactionTaskFinishedStatus(this);
        }
    }
}
