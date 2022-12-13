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
package sleeper.ingest.job.status;

import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.JobStatusUpdates;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessStatusUpdateRecord;
import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.ingest.job.status.IngestJobStatusTestData.defaultUpdateTime;

public class WriteToMemoryIngestJobStatusStore implements IngestJobStatusStore {
    private final Map<String, List<ProcessStatusUpdateRecord>> jobIdToUpdateRecords = new HashMap<>();

    @Override
    public void jobStarted(String taskId, IngestJob job, Instant startTime) {
        ProcessStatusUpdateRecord updateRecord = new ProcessStatusUpdateRecord(job.getId(), null,
                IngestJobStartedStatus.updateAndStartTime(job, defaultUpdateTime(startTime), startTime), taskId);
        jobIdToUpdateRecords.computeIfAbsent(job.getId(), jobId -> new ArrayList<>()).add(updateRecord);
    }

    @Override
    public void jobFinished(String taskId, IngestJob job, RecordsProcessedSummary summary) {
        ProcessStatusUpdateRecord updateRecord = new ProcessStatusUpdateRecord(job.getId(), null,
                ProcessFinishedStatus.updateTimeAndSummary(defaultUpdateTime(summary.getFinishTime()), summary), taskId);
        if (!jobIdToUpdateRecords.containsKey(job.getId())) {
            throw new IllegalStateException("Job not started: " + job.getId());
        }
        jobIdToUpdateRecords.get(job.getId()).add(updateRecord);
    }

    @Override
    public List<IngestJobStatus> getAllJobs(String tableName) {
        return jobIdToUpdateRecords.entrySet().stream()
                .map(entry -> JobStatusUpdates.from(entry.getKey(), entry.getValue()))
                .map(IngestJobStatus::from)
                .collect(Collectors.toList());
    }
}
