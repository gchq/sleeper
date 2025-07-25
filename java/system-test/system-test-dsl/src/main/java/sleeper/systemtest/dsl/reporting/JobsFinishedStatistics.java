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

package sleeper.systemtest.dsl.reporting;

import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobStatus;
import sleeper.core.tracker.job.run.AverageRowRate;
import sleeper.core.tracker.job.run.JobRunReport;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public class JobsFinishedStatistics {
    private final int numJobs;
    private final int numFinishedJobs;
    private final int numJobRuns;
    private final int numFinishedJobRuns;
    private final AverageRowRate averageRowRate;

    private JobsFinishedStatistics(Builder builder) {
        numJobs = builder.numJobs;
        numFinishedJobs = builder.numFinishedJobs;
        numJobRuns = builder.numJobRuns;
        numFinishedJobRuns = builder.numFinishedJobRuns;
        averageRowRate = builder.averageRowRate;
    }

    public static JobsFinishedStatistics fromIngestJobs(List<IngestJobStatus> jobs) {
        return builder().jobs(jobs, IngestJobStatus::isAnyRunSuccessful, IngestJobStatus::getRunsLatestFirst).build();
    }

    public static JobsFinishedStatistics fromCompactionJobs(List<CompactionJobStatus> jobs) {
        return builder().jobs(jobs, CompactionJobStatus::isAnyRunSuccessful, CompactionJobStatus::getRunsLatestFirst).build();
    }

    private static Builder builder() {
        return new Builder();
    }

    public boolean isAllFinishedOneRunEach(int expectedJobs) {
        return numJobs == expectedJobs
                && numFinishedJobs == expectedJobs
                && numJobRuns == expectedJobs
                && numFinishedJobRuns == expectedJobs;
    }

    public boolean isAverageRunRowsPerSecondInRange(double minRate, double maxRate) {
        return averageRowRate.getAverageRunRowsReadPerSecond() > minRate
                && averageRowRate.getAverageRunRowsWrittenPerSecond() > minRate
                && averageRowRate.getAverageRunRowsReadPerSecond() < maxRate
                && averageRowRate.getAverageRunRowsWrittenPerSecond() < maxRate;
    }

    @Override
    public String toString() {
        return "JobsFinishedStatistics{" +
                "numJobs=" + numJobs +
                ", numFinishedJobs=" + numFinishedJobs +
                ", numJobRuns=" + numJobRuns +
                ", numFinishedJobRuns=" + numFinishedJobRuns +
                ", averageRowRate=" + averageRowRate +
                '}';
    }

    private static final class Builder {
        private int numJobs;
        private int numFinishedJobs;
        private int numJobRuns;
        private int numFinishedJobRuns;
        private AverageRowRate averageRowRate;

        private Builder() {
        }

        public <T> Builder jobs(List<T> jobs, Predicate<T> isJobFinished, Function<T, List<? extends JobRunReport>> getJobRuns) {
            this.numJobs = jobs.size();
            this.numFinishedJobs = (int) jobs.stream().filter(isJobFinished).count();
            this.numJobRuns = jobs.stream()
                    .mapToInt(job -> getJobRuns.apply(job).size())
                    .sum();
            this.numFinishedJobRuns = jobs.stream()
                    .mapToInt(job -> (int) getJobRuns.apply(job).stream().filter(JobRunReport::isFinished).count())
                    .sum();
            this.averageRowRate = AverageRowRate.of(jobs.stream()
                    .filter(isJobFinished)
                    .flatMap(job -> getJobRuns.apply(job).stream()));
            return this;
        }

        public JobsFinishedStatistics build() {
            return new JobsFinishedStatistics(this);
        }
    }
}
