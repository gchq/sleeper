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

package sleeper.ingest.task;

import sleeper.core.record.process.AverageRecordRate;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;

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
    private final long totalRecordsRead;
    private final long totalRecordsWritten;
    private final double recordsReadPerSecond;
    private final double recordsWrittenPerSecond;

    private IngestTaskFinishedStatus(Builder builder) {
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
     * Creates a records processed summary using a start time.
     *
     * @param  startTime the start time
     * @return           a {@link RecordProcessedSummary}
     */
    public RecordsProcessedSummary asSummary(Instant startTime) {
        return new RecordsProcessedSummary(
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
        IngestTaskFinishedStatus that = (IngestTaskFinishedStatus) o;
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
        return "IngestTaskFinishedStatus{" +
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
     * Builder class for ingest task finished status objects.
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
         * Sets the total records read.
         *
         * @param  totalRecordsRead the total records read
         * @return                  the builder
         */
        public Builder totalRecordsRead(long totalRecordsRead) {
            this.totalRecordsRead = totalRecordsRead;
            return this;
        }

        /**
         * Sets the total records written.
         *
         * @param  totalRecordsWritten the total records written
         * @return                     the builder
         */
        public Builder totalRecordsWritten(long totalRecordsWritten) {
            this.totalRecordsWritten = totalRecordsWritten;
            return this;
        }

        /**
         * Sets the records read per second.
         *
         * @param  recordsReadPerSecond the records read per second
         * @return                      the builder
         */
        public Builder recordsReadPerSecond(double recordsReadPerSecond) {
            this.recordsReadPerSecond = recordsReadPerSecond;
            return this;
        }

        /**
         * Sets the records written per second.
         *
         * @param  recordsWrittenPerSecond the records written per second
         * @return                         the builder
         */
        public Builder recordsWrittenPerSecond(double recordsWrittenPerSecond) {
            this.recordsWrittenPerSecond = recordsWrittenPerSecond;
            return this;
        }

        /**
         * Adds a job summary to calculate the average record rates.
         *
         * @param  jobSummary the job summary
         * @return            the builder
         */
        public Builder addJobSummary(RecordsProcessedSummary jobSummary) {
            rateBuilder.summary(jobSummary);
            return this;
        }

        /**
         * Adds job summaries to calculate the average record rates.
         *
         * @param  jobSummaries the job summaries
         * @return              the builder
         */
        public Builder jobSummaries(Stream<RecordsProcessedSummary> jobSummaries) {
            rateBuilder.summaries(jobSummaries);
            return this;
        }

        Builder finish(Instant finishTime) {
            AverageRecordRate rate = rateBuilder.build();
            totalJobRuns = rate.getRunCount();
            totalRecordsRead = rate.getRecordsRead();
            totalRecordsWritten = rate.getRecordsWritten();
            timeSpentOnJobs = rate.getTotalDuration();
            recordsReadPerSecond = rate.getRecordsReadPerSecond();
            recordsWrittenPerSecond = rate.getRecordsWrittenPerSecond();
            this.finishTime = finishTime;
            return this;
        }

        public IngestTaskFinishedStatus build() {
            return new IngestTaskFinishedStatus(this);
        }
    }
}
