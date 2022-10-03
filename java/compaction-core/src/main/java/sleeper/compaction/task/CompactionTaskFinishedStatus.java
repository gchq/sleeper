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

package sleeper.compaction.task;

import sleeper.compaction.job.status.CompactionJobStatus;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class CompactionTaskFinishedStatus {
    private final Instant finishTime;
    private final Integer totalJobs;
    private final double totalRuntime;
    private final long totalRecordsRead;
    private final long totalRecordsWritten;
    private final double recordsReadPerSecond;
    private final double recordsWrittenPerSecond;

    public Instant getFinishTime() {
        return finishTime;
    }

    public Integer getTotalJobs() {
        return totalJobs;
    }

    public double getTotalRuntime() {
        return totalRuntime;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactionTaskFinishedStatus that = (CompactionTaskFinishedStatus) o;
        return Double.compare(that.totalRuntime, totalRuntime) == 0
                && Double.compare(that.totalRecordsRead, totalRecordsRead) == 0
                && Double.compare(that.totalRecordsWritten, totalRecordsWritten) == 0
                && Double.compare(that.recordsReadPerSecond, recordsReadPerSecond) == 0
                && Double.compare(that.recordsWrittenPerSecond, recordsWrittenPerSecond) == 0
                && Objects.equals(finishTime, that.finishTime)
                && Objects.equals(totalJobs, that.totalJobs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(finishTime, totalJobs, totalRuntime,
                totalRecordsRead, totalRecordsWritten, recordsReadPerSecond, recordsWrittenPerSecond);
    }

    @Override
    public String toString() {
        return "CompactionTaskFinishedStatus{" +
                "finishTime=" + finishTime +
                ", totalJobs=" + totalJobs +
                ", totalRuntime=" + totalRuntime +
                ", totalRecordsRead=" + totalRecordsRead +
                ", totalRecordsWritten=" + totalRecordsWritten +
                ", recordsReadPerSecond=" + recordsReadPerSecond +
                ", recordsWrittenPerSecond=" + recordsWrittenPerSecond +
                '}';
    }

    private CompactionTaskFinishedStatus(Builder builder) {
        finishTime = builder.finishTime;
        totalJobs = builder.totalJobs;
        totalRuntime = builder.totalRuntime;
        totalRecordsRead = builder.totalRecordsRead;
        totalRecordsWritten = builder.totalRecordsWritten;
        recordsReadPerSecond = builder.recordsReadPerSecond;
        recordsWrittenPerSecond = builder.recordsWrittenPerSecond;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static CompactionTaskFinishedStatus fromJobList(List<CompactionJobStatus> jobStatusList, Instant finishTime, double duration) {
        if (jobStatusList.stream().anyMatch(job -> !job.isFinished())) {
            throw new IllegalArgumentException("Some jobs are not finished in provided list");
        }
        long totalLinesRead = jobStatusList.stream().mapToLong(job -> job.getFinishedSummary().getLinesRead()).sum();
        long totalLinesWritten = jobStatusList.stream().mapToLong(job -> job.getFinishedSummary().getLinesWritten()).sum();
        return new Builder()
                .finishTime(finishTime)
                .totalJobs(jobStatusList.size())
                .totalRuntime(duration)
                .totalRecordsRead(totalLinesRead)
                .totalRecordsWritten(totalLinesWritten)
                .recordsReadPerSecond(totalLinesRead / duration)
                .recordsWrittenPerSecond(totalLinesWritten / duration)
                .build();
    }

    public static final class Builder {
        private Instant finishTime;
        private Integer totalJobs;
        private double totalRuntime;
        private long totalRecordsRead;
        private long totalRecordsWritten;
        private double recordsReadPerSecond;
        private double recordsWrittenPerSecond;

        private Builder() {
        }

        public Builder finishTime(Instant finishTime) {
            this.finishTime = finishTime;
            return this;
        }

        public Builder totalJobs(Integer totalJobs) {
            this.totalJobs = totalJobs;
            return this;
        }

        public Builder totalRuntime(double totalRuntime) {
            this.totalRuntime = totalRuntime;
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

        public CompactionTaskFinishedStatus build() {
            return new CompactionTaskFinishedStatus(this);
        }
    }
}
