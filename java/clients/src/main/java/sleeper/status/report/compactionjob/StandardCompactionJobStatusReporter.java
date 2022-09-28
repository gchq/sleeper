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

import java.io.PrintStream;
import java.util.List;
import java.util.stream.Collectors;

public class StandardCompactionJobStatusReporter implements CompactionJobStatusReporter {
    private final PrintStream out;

    public StandardCompactionJobStatusReporter() {
        this.out = System.out;
    }

    public StandardCompactionJobStatusReporter(PrintStream out) {
        this.out = out;
    }

    public void report(List<CompactionJobStatus> jobStatusList, QueryType queryType) {
        out.println("\nCompaction Job Status Report:");
        out.println("--------------------------");
        printSummary(jobStatusList, queryType);
        if (!queryType.equals(QueryType.DETAILED)) {
            out.println("--------------------------");
            printHeaders();
            out.println();
            jobStatusList.forEach(this::printJobRow);
        }
    }

    private void printSummary(List<CompactionJobStatus> jobStatusList, QueryType queryType) {
        if (queryType.equals(QueryType.RANGE)) {
            printRangeSummary(jobStatusList);
        }
        if (queryType.equals(QueryType.DETAILED)) {
            printDetailedSummary(jobStatusList);
        }
        if (queryType.equals(QueryType.UNFINISHED)) {
            printUnfinishedSummary(jobStatusList);
        }
        if (queryType.equals(QueryType.ALL)) {
            printAllSummary(jobStatusList);
        }
    }

    private void printRangeSummary(List<CompactionJobStatus> jobStatusList) {
        out.printf("Total jobs finished between defined range: %d%n",
                jobStatusList.size());
    }

    private void printDetailedSummary(List<CompactionJobStatus> jobStatusList) {
        if (jobStatusList.isEmpty()) {
            out.printf("No job found with provided jobId%n");
            out.printf("--------------------------%n");
        } else {
            jobStatusList.forEach(this::printSingleJobSummary);
        }
    }

    private void printSingleJobSummary(CompactionJobStatus jobStatus) {
        out.printf("Details for job %s:%n", jobStatus.getJobId());
        out.printf("State: %s%n", getState(jobStatus));
        out.printf("Creation Time: %s%n", jobStatus.getCreateUpdateTime().toString());
        out.printf("Partition ID: %s%n", jobStatus.getPartitionId());
        out.printf("Child partition IDs: %s%n", jobStatus.getChildPartitionIds().toString());
        if (jobStatus.isStarted()) {
            out.println();
            out.printf("Start Time: %s%n", jobStatus.getStartTime());
            out.printf("Start Update Time: %s%n", jobStatus.getStartUpdateTime());
        }
        if (jobStatus.isFinished()) {
            out.println();
            out.printf("Finish Time: %s%n", jobStatus.getFinishTime());
            out.printf("Duration: %.1fs%n", jobStatus.getFinishedSummary().getDurationInSeconds());
            out.printf("Lines Read: %d%n", jobStatus.getFinishedSummary().getLinesRead());
            out.printf("Lines Written: %d%n", jobStatus.getFinishedSummary().getLinesWritten());
            out.printf("Read Rate (reads per second): %.1f%n", jobStatus.getFinishedSummary().getRecordsReadPerSecond());
            out.printf("Write Rate (writes per second): %.1f%n", jobStatus.getFinishedSummary().getRecordsWrittenPerSecond());
        }
        out.println("--------------------------");
    }

    private void printUnfinishedSummary(List<CompactionJobStatus> jobStatusList) {
        out.printf("Total unfinished jobs: %d%n", jobStatusList.size());
        out.printf("Total unfinished jobs in progress: %d%n",
                jobStatusList.stream().filter(CompactionJobStatus::isStarted).count());
        out.printf("Total unfinished jobs not started: %d%n",
                jobStatusList.size() - jobStatusList.stream().filter(CompactionJobStatus::isStarted).count());
    }

    private void printAllSummary(List<CompactionJobStatus> jobStatusList) {
        List<CompactionJobStatus> splittingJobs = jobStatusList.stream().filter(CompactionJobStatus::isSplittingCompaction).collect(Collectors.toList());
        List<CompactionJobStatus> standardJobs = jobStatusList.stream().filter(job -> !job.isSplittingCompaction()).collect(Collectors.toList());
        out.printf("Total jobs: %d%n", jobStatusList.size());
        out.println();
        out.printf("Total standard jobs: %d%n", standardJobs.size());
        out.printf("Total standard jobs pending: %d%n", standardJobs.stream().filter(job -> getState(job).equals("PENDING")).count());
        out.printf("Total standard jobs in progress: %d%n", standardJobs.stream().filter(job -> getState(job).equals("IN PROGRESS")).count());
        out.printf("Total standard jobs finished: %d%n", standardJobs.stream().filter(job -> getState(job).equals("FINISHED")).count());
        out.println();
        out.printf("Total splitting jobs: %d%n", splittingJobs.size());
        out.printf("Total splitting jobs pending: %d%n", splittingJobs.stream().filter(job -> getState(job).equals("PENDING")).count());
        out.printf("Total splitting jobs in progress: %d%n", splittingJobs.stream().filter(job -> getState(job).equals("IN PROGRESS")).count());
        out.printf("Total splitting jobs finished: %d%n", splittingJobs.stream().filter(job -> getState(job).equals("FINISHED")).count());
    }

    private void printHeaders() {
        out.printf("%-11s|", "STATE");
        out.printf("%-24s|", "CREATE_TIME");
        out.printf("%-36s|", "JOB_ID");
        out.printf("%-36s|", "PARTITION_ID");
        out.printf("%-20s|", "CHILD_IDS");
        out.printf("%-24s|", "START_TIME");
        out.printf("%-24s|", "START_UPDATE_TIME");
        out.printf("%-24s|", "FINISH_TIME");
        out.printf("%-20s|", "DURATION (s)");
        out.printf("%-20s|", "LINES_READ");
        out.printf("%-20s|", "LINES_WRITTEN");
        out.printf("%-20s|", "READ_RATE (read/s)");
        out.printf("%-20s", "WRITE_RATE (write/s)");
    }

    public void printJobRow(CompactionJobStatus status) {
        out.printf("%-11s|", getState(status));
        out.printf("%-24s|", status.getCreateUpdateTime().toString());
        out.printf("%-36s|", status.getJobId());
        out.printf("%-36s|", status.getPartitionId());
        out.printf("%-20s|", status.getChildPartitionIds());

        out.printf("%-24s|", status.isStarted() ? status.getStartTime() : "");
        out.printf("%-24s|", status.isStarted() ? status.getStartUpdateTime() : "");

        out.printf("%-24s|", status.isFinished() ? status.getFinishTime() : "");
        out.printf("%-20s|", status.isFinished() ? status.getFinishedSummary().getDurationInSeconds() : "");
        out.printf("%-20s|", status.isFinished() ? status.getFinishedSummary().getLinesRead() : "");
        out.printf("%-20s|", status.isFinished() ? status.getFinishedSummary().getLinesWritten() : "");
        out.printf("%-20s|", status.isFinished() ? status.getFinishedSummary().getRecordsReadPerSecond() : "");
        out.printf("%-20s%n", status.isFinished() ? status.getFinishedSummary().getRecordsWrittenPerSecond() : "");
    }

    public static String getState(CompactionJobStatus status) {
        if (status.isFinished()) {
            return "FINISHED";
        }
        if (status.isStarted()) {
            return "IN PROGRESS";
        }
        return "PENDING";
    }
}
