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

package sleeper.compaction.job;

import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobStartedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessStatusUpdate;
import sleeper.core.record.process.status.TestProcessStatusUpdateRecords;

import java.time.Instant;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.List;

import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.records;

public class CompactionJobStatusTestData {
    private CompactionJobStatusTestData() {
    }

    public static CompactionJobStatus jobCreated(CompactionJob job, Instant createdTime, ProcessRun... runsLatestFirst) {
        return CompactionJobStatus.builder()
                .jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, createdTime))
                .jobRunsLatestFirst(Arrays.asList(runsLatestFirst))
                .build();
    }

    public static ProcessRun startedCompactionRun(String taskId, Instant startTime) {
        return ProcessRun.started(taskId, startedCompactionStatus(startTime));
    }

    public static ProcessRun finishedCompactionRun(String taskId, RecordsProcessedSummary summary) {
        return ProcessRun.finished(taskId,
                startedCompactionStatus(summary.getStartTime()),
                finishedCompactionStatus(summary));
    }

    public static CompactionJobStartedStatus startedCompactionStatus(Instant startTime) {
        return CompactionJobStartedStatus.startAndUpdateTime(startTime, defaultUpdateTime(startTime));
    }

    public static ProcessFinishedStatus finishedCompactionStatus(RecordsProcessedSummary summary) {
        return ProcessFinishedStatus.updateTimeAndSummary(defaultUpdateTime(summary.getFinishTime()), summary);
    }

    public static Instant defaultUpdateTime(Instant time) {
        return time.with(ChronoField.MILLI_OF_SECOND, 123);
    }

    public static CompactionJobStatus jobStatusFromUpdates(ProcessStatusUpdate... updates) {
        return jobStatusFrom(records().fromUpdates(updates));
    }

    public static List<CompactionJobStatus> jobStatusListFromUpdates(
            TestProcessStatusUpdateRecords.TaskUpdates... updates) {
        return CompactionJobStatus.listFrom(records().fromUpdates(updates).stream());
    }

    public static CompactionJobStatus jobStatusFrom(TestProcessStatusUpdateRecords records) {
        List<CompactionJobStatus> built = CompactionJobStatus.listFrom(records.stream());
        if (built.size() != 1) {
            throw new IllegalStateException("Expected single status");
        }
        return built.get(0);
    }
}
