/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.suite.dsl.reports;

import sleeper.core.record.process.AverageRecordRate;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.ingest.job.status.IngestJobStatus;

import java.util.List;
import java.util.stream.Stream;

public class JobsFinishedStatistics {
    private final int numJobs;
    private final int numFinishedJobs;
    private final int numJobRuns;
    private final int numFinishedJobRuns;
    private final AverageRecordRate averageRecordRate;

    public JobsFinishedStatistics(List<IngestJobStatus> jobs) {
        this.numJobs = jobs.size();
        this.numFinishedJobs = (int) finished(jobs).count();
        this.numJobRuns = jobs.stream()
                .mapToInt(job -> job.getJobRuns().size())
                .sum();
        this.numFinishedJobRuns = jobs.stream()
                .mapToInt(job -> (int) job.getJobRuns().stream().filter(ProcessRun::isFinished).count())
                .sum();
        this.averageRecordRate = AverageRecordRate.of(finished(jobs).flatMap(job -> job.getJobRuns().stream()));
    }

    public boolean isAllFinishedOneRunEach(int expectedJobs) {
        return numJobs == expectedJobs
                && numFinishedJobs == expectedJobs
                && numJobRuns == expectedJobs
                && numFinishedJobRuns == expectedJobs;
    }

    public boolean isMinAverageRunRecordsPerSecond(double minRate) {
        return averageRecordRate.getAverageRunRecordsReadPerSecond() > minRate
                && averageRecordRate.getAverageRunRecordsWrittenPerSecond() > minRate;
    }

    private static Stream<IngestJobStatus> finished(List<IngestJobStatus> jobs) {
        return jobs.stream().filter(IngestJobStatus::isFinished);
    }
}
