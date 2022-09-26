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

package sleeper.status.report.compactionjob;

import sleeper.compaction.job.status.CompactionJobStatus;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class CompactionJobStatusReporter {
    public enum QueryType {
        RANGE,
        DETAILED,
        UNFINISHED
    }

    private Instant startRange = Instant.now().minus(6, ChronoUnit.HOURS);
    private Instant endRange = Instant.now();

    public CompactionJobStatusReporter() {
    }

    public String report(List<CompactionJobStatus> jobStatusList, QueryType queryType) {
        StringBuilder sb = new StringBuilder();
        sb.append("\nCompaction Job Status Report:\n");
        sb.append("--------------------------\n");
        sb.append(printSummary(jobStatusList, queryType));
        if (!queryType.equals(QueryType.DETAILED)) {
            sb.append("--------------------------\n");
            sb.append(printHeaders()).append('\n');
            jobStatusList.forEach(s -> sb.append(verboseString(s)));
        }
        return sb.toString();
    }

    public void setRange(Instant startRange, Instant endRange) {
        this.startRange = startRange;
        this.endRange = endRange;
    }

    public String verboseString(CompactionJobStatus jobStatus) {
        return CompactionJobStatusResult.from(jobStatus).toString();
    }

    private String printSummary(List<CompactionJobStatus> jobStatusList, QueryType queryType) {
        if (queryType.equals(QueryType.RANGE)) {
            return printRangeSummary(jobStatusList);
        }
        if (queryType.equals(QueryType.DETAILED)) {
            return printDetailedSummary(jobStatusList);
        }
        if (queryType.equals(QueryType.UNFINISHED)) {
            return printUnfinishedSummary(jobStatusList);
        }
        return "";
    }

    private String printRangeSummary(List<CompactionJobStatus> jobStatusList) {
        StringBuilder sb = new StringBuilder();
        sb.append("Total jobs finished between ").append(startRange.toString());
        sb.append(" and ").append(endRange.toString()).append(": ")
                .append(jobStatusList.size())
                .append('\n');
        return sb.toString();
    }

    private String printDetailedSummary(List<CompactionJobStatus> jobStatusList) {
        StringBuilder sb = new StringBuilder();
        jobStatusList.forEach(job -> sb.append(printSingleJobSummary(job)));
        return sb.toString();
    }

    private String printSingleJobSummary(CompactionJobStatus jobStatus) {
        StringBuilder sb = new StringBuilder();
        sb.append("Details for job ").append(jobStatus.getJobId()).append(":\n");
        sb.append("State: ").append(CompactionJobStatusResult.getState(jobStatus)).append('\n');
        sb.append("Creation Time: ").append(jobStatus.getCreateUpdateTime().toString()).append('\n');
        sb.append("Partition ID: ").append(jobStatus.getPartitionId()).append('\n');
        sb.append("Child partition IDs: ").append(jobStatus.getChildPartitionIds().toString()).append('\n');
        if (jobStatus.isStarted()) {
            sb.append("\nStart Time: ").append(jobStatus.getStartTime()).append('\n');
            sb.append("Start Update Time: ").append(jobStatus.getStartUpdateTime()).append('\n');
        }
        if (jobStatus.isFinished()) {
            sb.append("\nFinish Time: ").append(jobStatus.getFinishTime()).append('\n');
            sb.append("Duration: ").append(jobStatus.getFinishedSummary().getDurationInSeconds()).append("s\n");
            sb.append("Lines Read: ").append(jobStatus.getFinishedSummary().getLinesRead()).append('\n');
            sb.append("Lines Written: ").append(jobStatus.getFinishedSummary().getLinesWritten()).append('\n');
            sb.append("Read Rate (reads per second): ").append(jobStatus.getFinishedSummary().getRecordsReadPerSecond()).append('\n');
            sb.append("Write Rate (writes per second): ").append(jobStatus.getFinishedSummary().getRecordsWrittenPerSecond()).append('\n');
        }
        sb.append("--------------------------\n");
        return sb.toString();
    }

    private String printUnfinishedSummary(List<CompactionJobStatus> jobStatusList) {
        StringBuilder sb = new StringBuilder();
        sb.append("Total unfinished jobs: ").append(jobStatusList.size()).append('\n');
        sb.append("Total unfinished jobs in progress: ")
                .append(jobStatusList.stream().filter(CompactionJobStatus::isStarted).count())
                .append('\n');
        sb.append("Total unfinished jobs not started: ")
                .append(jobStatusList.size() - jobStatusList.stream().filter(CompactionJobStatus::isStarted).count())
                .append('\n');
        return sb.toString();
    }

    private String printHeaders() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%-11s", "STATE")).append('|');
        sb.append(String.format("%-24s", "CREATE_TIME")).append('|');
        sb.append(String.format("%-36s", "JOB_ID")).append('|');
        sb.append(String.format("%-36s", "PARTITION_ID")).append('|');
        sb.append(String.format("%-20s", "CHILD_IDS")).append('|');
        sb.append(String.format("%-24s", "START_TIME")).append('|');
        sb.append(String.format("%-24s", "START_UPDATE_TIME")).append('|');
        sb.append(String.format("%-24s", "FINISH_TIME")).append('|');
        sb.append(String.format("%-20s", "DURATION (s)")).append('|');
        sb.append(String.format("%-20s", "LINES_READ")).append('|');
        sb.append(String.format("%-20s", "LINES_WRITTEN")).append('|');
        sb.append(String.format("%-20s", "READ_RATE (read/s)")).append('|');
        sb.append(String.format("%-20s", "WRITE_RATE (write/s)"));
        return sb.toString();
    }

    public boolean isFinishedInRange(CompactionJobStatus jobStatus) {
        return jobStatus.getFinishTime().isAfter(startRange) && jobStatus.getFinishTime().isBefore(endRange);
    }
}
