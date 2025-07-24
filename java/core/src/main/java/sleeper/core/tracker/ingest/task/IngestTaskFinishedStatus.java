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

package sleeper.core.tracker.ingest.task;

import sleeper.core.tracker.job.run.AverageRecordRate;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.RowsProcessed;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * A status for ingest tasks that have finished.
 */
public class IngestTaskFinishedStatus {
    private final Instant finishTime;
    private final int totalJobRuns;
    private final Duration timeSpentOnJobs;
    private final long totalRowsRead;
    private final long totalRowsWritten;
    private final double rowsReadPerSecond;
    private final double rowsWrittenPerSecond;

    private IngestTaskFinishedStatus(Builder builder) {
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
     * Creates a rows processed summary using a start time.
     *
     * @param  startTime the start time
     * @return           a {@link JobRunSummary}
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
        IngestTaskFinishedStatus that = (IngestTaskFinishedStatus) o;
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
        return "IngestTaskFinishedStatus{" +
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
     * Builder class for ingest task finished status objects.
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
         * Sets the finish time.
         *
         * @param  finishTime the finish time
         * @return            the builder
         */
        public Builder finishTime(Instant finishTime) {
            this.finishTime = finishTime;
            return this;
        }

        /**
         * Sets the total job runs.
         *
         * @param  totalJobRuns the total job runs
         * @return              the builder
         */
        public Builder totalJobRuns(int totalJobRuns) {
            this.totalJobRuns = totalJobRuns;
            return this;
        }

        /**
         * Sets the time spent on jobs.
         *
         * @param  timeSpentOnJobs the time spent on jobs
         * @return                 the builder
         */
        public Builder timeSpentOnJobs(Duration timeSpentOnJobs) {
            this.timeSpentOnJobs = timeSpentOnJobs;
            return this;
        }

        /**
         * Sets the total rows read.
         *
         * @param  totalRowsRead the total rows read
         * @return               the builder
         */
        public Builder totalRowsRead(long totalRowsRead) {
            this.totalRowsRead = totalRowsRead;
            return this;
        }

        /**
         * Sets the total rows written.
         *
         * @param  totalRowsWritten the total rows written
         * @return                  the builder
         */
        public Builder totalRowsWritten(long totalRowsWritten) {
            this.totalRowsWritten = totalRowsWritten;
            return this;
        }

        /**
         * Sets the rows read per second.
         *
         * @param  rowsReadPerSecond the rows read per second
         * @return                   the builder
         */
        public Builder rowsReadPerSecond(double rowsReadPerSecond) {
            this.rowsReadPerSecond = rowsReadPerSecond;
            return this;
        }

        /**
         * Sets the rows written per second.
         *
         * @param  rowsWrittenPerSecond the rows written per second
         * @return                      the builder
         */
        public Builder rowsWrittenPerSecond(double rowsWrittenPerSecond) {
            this.rowsWrittenPerSecond = rowsWrittenPerSecond;
            return this;
        }

        /**
         * Adds a job summary to calculate the average row rates.
         *
         * @param  jobSummary the job summary
         * @return            the builder
         */
        public Builder addJobSummary(JobRunSummary jobSummary) {
            rateBuilder.summary(jobSummary);
            return this;
        }

        /**
         * Adds job summaries to calculate the average row rates.
         *
         * @param  jobSummaries the job summaries
         * @return              the builder
         */
        public Builder jobSummaries(Stream<JobRunSummary> jobSummaries) {
            rateBuilder.summaries(jobSummaries);
            return this;
        }

        Builder finish(Instant finishTime) {
            AverageRecordRate rate = rateBuilder.build();
            totalJobRuns = rate.getRunCount();
            totalRowsRead = rate.getRecordsRead();
            totalRowsWritten = rate.getRecordsWritten();
            timeSpentOnJobs = rate.getTotalDuration();
            rowsReadPerSecond = rate.getRecordsReadPerSecond();
            rowsWrittenPerSecond = rate.getRecordsWrittenPerSecond();
            this.finishTime = finishTime;
            return this;
        }

        public IngestTaskFinishedStatus build() {
            return new IngestTaskFinishedStatus(this);
        }
    }
}
