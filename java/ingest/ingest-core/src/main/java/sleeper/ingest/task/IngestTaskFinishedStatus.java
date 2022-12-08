/*
 * Copyright 2022 Crown Copyright
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

import java.time.Instant;
import java.util.Objects;

public class IngestTaskFinishedStatus {
    private final Instant finishTime;
    private final int totalJobRuns;
    private final double totalRuntimeInSeconds;
    private final long totalRecordsRead;
    private final long totalRecordsWritten;
    private final double recordsReadPerSecond;
    private final double recordsWrittenPerSecond;

    private IngestTaskFinishedStatus(Builder builder) {
        finishTime = Objects.requireNonNull(builder.finishTime, "finishTime must not be null");
        totalJobRuns = builder.totalJobRuns;
        totalRuntimeInSeconds = builder.totalRuntimeInSeconds;
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

    public double getTotalRuntimeInSeconds() {
        return totalRuntimeInSeconds;
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

    public RecordsProcessedSummary asSummary(Instant startTime) {
        return new RecordsProcessedSummary(
                new RecordsProcessed(totalRecordsRead, totalRecordsWritten),
                startTime, finishTime);
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
        return Double.compare(that.totalRuntimeInSeconds, totalRuntimeInSeconds) == 0
                && Double.compare(that.totalRecordsRead, totalRecordsRead) == 0
                && Double.compare(that.totalRecordsWritten, totalRecordsWritten) == 0
                && Double.compare(that.recordsReadPerSecond, recordsReadPerSecond) == 0
                && Double.compare(that.recordsWrittenPerSecond, recordsWrittenPerSecond) == 0
                && Objects.equals(finishTime, that.finishTime)
                && Objects.equals(totalJobRuns, that.totalJobRuns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(finishTime, totalJobRuns, totalRuntimeInSeconds,
                totalRecordsRead, totalRecordsWritten, recordsReadPerSecond, recordsWrittenPerSecond);
    }

    @Override
    public String toString() {
        return "IngestTaskFinishedStatus{" +
                "finishTime=" + finishTime +
                ", totalJobs=" + totalJobRuns +
                ", totalRuntimeInSeconds=" + totalRuntimeInSeconds +
                ", totalRecordsRead=" + totalRecordsRead +
                ", totalRecordsWritten=" + totalRecordsWritten +
                ", recordsReadPerSecond=" + recordsReadPerSecond +
                ", recordsWrittenPerSecond=" + recordsWrittenPerSecond +
                '}';
    }

    public static final class Builder {
        private Instant finishTime;
        private int totalJobRuns;
        private double totalRuntimeInSeconds;
        private long totalRecordsRead;
        private long totalRecordsWritten;
        private double recordsReadPerSecond;
        private double recordsWrittenPerSecond;
        private final AverageRecordRate.Builder rateBuilder = AverageRecordRate.builder();

        private Builder() {
        }

        public Builder finishTime(Instant finishTime) {
            this.finishTime = finishTime;
            return this;
        }

        public Builder totalJobRuns(int totalJobRuns) {
            this.totalJobRuns = totalJobRuns;
            return this;
        }

        public Builder totalRuntimeInSeconds(double totalRuntimeInSeconds) {
            this.totalRuntimeInSeconds = totalRuntimeInSeconds;
            return this;
        }

        public Builder totalRecordsRead(long totalRecordsRead) {
            this.totalRecordsRead = totalRecordsRead;
            return this;
        }

        public Builder totalRecordsWritten(long totalRecordsWritten) {
            this.totalRecordsWritten = totalRecordsWritten;
            return this;
        }

        public Builder recordsReadPerSecond(double recordsReadPerSecond) {
            this.recordsReadPerSecond = recordsReadPerSecond;
            return this;
        }

        public Builder recordsWrittenPerSecond(double recordsWrittenPerSecond) {
            this.recordsWrittenPerSecond = recordsWrittenPerSecond;
            return this;
        }

        public Builder addJobSummary(RecordsProcessedSummary jobSummary) {
            rateBuilder.summary(jobSummary);
            return this;
        }

        public Builder finish(Instant startTime, Instant finishTime) {
            AverageRecordRate rate = rateBuilder.startTime(startTime).finishTime(finishTime).build();
            totalJobRuns = rate.getRunCount();
            totalRecordsRead = rate.getRecordsRead();
            totalRecordsWritten = rate.getRecordsWritten();
            totalRuntimeInSeconds = rate.getTotalDurationInSeconds();
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
