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
 * This class holds details about the finished status of a CompactionTask.
 * This includes the finish time, the total job runs, time spent on jobs as well as other usefull summary items.
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
     * This method creates a new JobRubSummary using the provided startTime and the stored JobRun details.
     *
     * @param  startTime Instant the start time for the job run summary.
     * @return           JobRunSummary created using the provided start time.
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
     * Builder class for the CompactionTaskFinishedStatus.
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
         * Sets the finishTime and returns the builder.
         *
         * @param  finishTime Instant to be set.
         * @return            Builder containing current set values.
         */
        public Builder finishTime(Instant finishTime) {
            this.finishTime = finishTime;
            return this;
        }

        /**
         * Sets the totalJobRuns and returns the builder.
         *
         * @param  totalJobRuns int to be set.
         * @return              Builder containing current set values.
         */
        public Builder totalJobRuns(int totalJobRuns) {
            this.totalJobRuns = totalJobRuns;
            return this;
        }

        /**
         * Sets the timeSpentOnJobs and returns the builder.
         *
         * @param  timeSpentOnJobs Duration to be set.
         * @return                 Builder containing current set values.
         */
        public Builder timeSpentOnJobs(Duration timeSpentOnJobs) {
            this.timeSpentOnJobs = timeSpentOnJobs;
            return this;
        }

        /**
         * Sets the totalRecordsRead and returns the builder.
         *
         * @param  totalRecordsRead long to be set.
         * @return                  Builder containing current set values.
         */
        public Builder totalRecordsRead(long totalRecordsRead) {
            this.totalRecordsRead = totalRecordsRead;
            return this;
        }

        /**
         * Sets the totalRecordsWritten and returns the builder.
         *
         * @param  totalRecordsWritten long to be set.
         * @return                     Builder containing current set values.
         */
        public Builder totalRecordsWritten(long totalRecordsWritten) {
            this.totalRecordsWritten = totalRecordsWritten;
            return this;
        }

        /**
         * Sets the recordsReadPerSecond and returns the builder.
         *
         * @param  recordsReadPerSecond double to be set.
         * @return                      Builder containing current set values.
         */
        public Builder recordsReadPerSecond(double recordsReadPerSecond) {
            this.recordsReadPerSecond = recordsReadPerSecond;
            return this;
        }

        /**
         * Sets the recordsWrittenPerSecond and returns the builder.
         *
         * @param  recordsWrittenPerSecond double to be set.
         * @return                         Builder containing current set values.
         */
        public Builder recordsWrittenPerSecond(double recordsWrittenPerSecond) {
            this.recordsWrittenPerSecond = recordsWrittenPerSecond;
            return this;
        }

        /**
         * Adds the summary to the maintained details in the AvergaeRecordRate class and returns the builder.
         *
         * @param  jobSummary JobRunSummary containing details to be added to AvergaeRecordRate fields.
         * @return            Builder containing current set values.
         */
        public Builder addJobSummary(JobRunSummary jobSummary) {
            rateBuilder.summary(jobSummary);
            return this;
        }

        /**
         * Takes in a stream of JobRunSummaries and adds all their details to the existing values in the
         * AvergaeRecordRate class.
         *
         * @param  jobSummaries Stream of JobRunSummary to added.
         * @return              Builder containing current set values.
         */
        public Builder jobSummaries(Stream<JobRunSummary> jobSummaries) {
            rateBuilder.summaries(jobSummaries);
            return this;
        }

        /**
         * This method finished the status by extracting the final values from the AvergaeRecordRate and storing them in
         * this classes variables.
         *
         * @param  finishTime Instant of the finish time.
         * @return            Builder containing current set values.
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
