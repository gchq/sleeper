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

public class CompactionJobStatusReporter {
    private final PrintStream out;

    public enum QueryType {
        RANGE,
        SPECIFIC,
        UNFINISHED
    }

    public CompactionJobStatusReporter(PrintStream out) {
        this.out = out;
    }

    public String report(List<CompactionJobStatus> jobStatusList, QueryType queryType) {
        StringBuilder sb = new StringBuilder();
        sb.append("\nCompaction Job Status Report:\n");
        sb.append("--------------------------\n");
        sb.append(printSummary(jobStatusList, queryType));
        sb.append("--------------------------\n");
        return sb.toString();
    }

    public String verboseString(CompactionJobStatus jobStatus) {
        return CompactionJobStatusResult.from(jobStatus).toString();
    }

    private String printSummary(List<CompactionJobStatus> jobStatusList, QueryType queryType) {
        if (queryType.equals(QueryType.RANGE)) {
            return printRangeSummary(jobStatusList);
        }
        if (queryType.equals(QueryType.SPECIFIC)) {
            return printSpecificSummary(jobStatusList);
        }
        if (queryType.equals(QueryType.UNFINISHED)) {
            return printUnfinishedSummary(jobStatusList);
        }
        return "";
    }

    private String printRangeSummary(List<CompactionJobStatus> jobStatusList) {
        return "";
    }

    private String printSpecificSummary(List<CompactionJobStatus> jobStatusList) {
        return "";
    }

    private String printUnfinishedSummary(List<CompactionJobStatus> jobStatusList) {
        StringBuilder sb = new StringBuilder();
        sb.append("Total unfinished jobs: " + jobStatusList.size() + '\n');
        sb.append("Total unfinished jobs in progress: " + jobStatusList.stream().filter(CompactionJobStatus::isStarted).count() + '\n');
        sb.append("Total unfinished jobs not started: " + (jobStatusList.size() - jobStatusList.stream().filter(CompactionJobStatus::isStarted).count()) + '\n');
        return sb.toString();
    }

    private void printHeaders() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%11s", "STATE")).append('|');
        sb.append(String.format("%20s", "CREATE_TIME")).append('|');
        sb.append(String.format("%36s", "JOB_ID")).append('|');
        sb.append(String.format("%36s", "PARTITION_ID")).append('|');
        sb.append(String.format("%20s", "CHILD_IDS")).append('|');
        sb.append(String.format("%20s", "START_TIME")).append('|');
        sb.append(String.format("%20s", "START_UPDATE_TIME")).append('|');
        sb.append(String.format("%20s", "FINISH_TIME")).append('|');
        sb.append(String.format("%20s", "DURATION (s)")).append('|');
        sb.append(String.format("%20s", "LINES_READ")).append('|');
        sb.append(String.format("%20s", "LINES_WRITTEN")).append('|');
        sb.append(String.format("%20s", "READ_RATE (read/s)")).append('|');
        sb.append(String.format("%20s", "WRITE_RATE (write/s)"));
        out.println(sb);
    }
}
