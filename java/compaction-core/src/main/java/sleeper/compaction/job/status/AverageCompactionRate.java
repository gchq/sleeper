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
package sleeper.compaction.job.status;

import sleeper.compaction.job.CompactionJobSummary;

import java.util.List;

public class AverageCompactionRate {

    private final int jobCount;
    private final double recordsReadPerSecond;
    private final double recordsWrittenPerSecond;

    private AverageCompactionRate(Builder builder) {
        jobCount = builder.jobCount;
        recordsReadPerSecond = builder.totalRecordsRead / builder.totalDurationInSeconds;
        recordsWrittenPerSecond = builder.totalRecordsWritten / builder.totalDurationInSeconds;
    }

    public static AverageCompactionRate of(List<CompactionJobStatus> jobs) {
        return new Builder().jobs(jobs).build();
    }

    public int getJobCount() {
        return jobCount;
    }

    public double getRecordsReadPerSecond() {
        return recordsReadPerSecond;
    }

    public double getRecordsWrittenPerSecond() {
        return recordsWrittenPerSecond;
    }

    public static final class Builder {
        private int jobCount;
        private long totalRecordsRead;
        private long totalRecordsWritten;
        private double totalDurationInSeconds;

        private Builder() {
        }

        public Builder jobs(List<CompactionJobStatus> jobs) {
            jobs.stream()
                    .flatMap(job -> job.getJobRuns().stream())
                    .filter(CompactionJobRun::isFinished)
                    .forEach(run -> jobSummary(run.getFinishedSummary()));
            return this;
        }

        private void jobSummary(CompactionJobSummary summary) {
            jobCount++;
            totalRecordsRead += summary.getLinesRead();
            totalRecordsWritten += summary.getLinesWritten();
            totalDurationInSeconds += summary.getDurationInSeconds();
        }

        public AverageCompactionRate build() {
            return new AverageCompactionRate(this);
        }
    }
}
