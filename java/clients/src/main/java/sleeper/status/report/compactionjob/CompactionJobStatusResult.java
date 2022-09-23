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

public class CompactionJobStatusResult {
    private final CompactionJobStatus status;

    private CompactionJobStatusResult(CompactionJobStatus status) {
        this.status = status;
    }

    public static CompactionJobStatusResult from(CompactionJobStatus status) {
        return new CompactionJobStatusResult(status);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getState(status)).append('|');
        sb.append(status.getCreateUpdateTime().toString()).append('|');
        sb.append(status.getJobId()).append('|');
        sb.append(status.getPartitionId()).append('|');
        sb.append(status.getChildPartitionIds()).append('|');

        sb.append(status.isStarted() ? status.getStartTime() : "").append('|');
        sb.append(status.isStarted() ? status.getStartUpdateTime() : "").append('|');

        sb.append(status.isFinished() ? status.getFinishTime() : "").append('|');
        String linesRead = "";
        String linesWritten = "";
        String recordsReadPerSecond = "";
        String recordsWrittenPerSecond = "";
        String durationInSeconds = "";
        if (status.isFinished()) {
            linesRead = String.valueOf(status.getFinishedSummary().getLinesRead());
            linesWritten = String.valueOf(status.getFinishedSummary().getLinesWritten());
            recordsReadPerSecond = String.valueOf(status.getFinishedSummary().getRecordsReadPerSecond());
            recordsWrittenPerSecond = String.valueOf(status.getFinishedSummary().getRecordsWrittenPerSecond());
            durationInSeconds = String.valueOf(status.getFinishedSummary().getDurationInSeconds());
        }
        sb.append(durationInSeconds).append('|');
        sb.append(linesRead).append('|');
        sb.append(linesWritten).append('|');
        sb.append(recordsReadPerSecond).append('|');
        sb.append(recordsWrittenPerSecond);
        return sb.toString();
    }

    private static String getState(CompactionJobStatus status) {
        if (status.isFinished()) {
            return "FINISHED";
        }
        if (status.isStarted()) {
            return "IN PROGRESS";
        }
        return "PENDING";
    }
}
