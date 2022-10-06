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

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompactionJobStatusesBuilder {

    private final Map<String, CompactionJobCreatedStatus> createdById = new TreeMap<>(); // Order by job ID for output
    private final Map<String, List<CompactionJobRun>> jobRunsById = new HashMap<>();
    private final Map<String, Instant> expiryDateById = new HashMap<>();
    private final Map<String, Map<String, CompactionJobStartedStatus>> startedStatusMap = new HashMap<>();

    public CompactionJobStatusesBuilder jobCreated(
            String jobId, CompactionJobCreatedStatus statusUpdate) {
        createdById.put(jobId, statusUpdate);
        jobRunsById.put(jobId, new ArrayList<>());
        return this;
    }

    public CompactionJobStatusesBuilder jobStarted(
            String jobId, CompactionJobStartedStatus statusUpdate, String taskId) {
        if (!startedStatusMap.containsKey(jobId)) {
            startedStatusMap.put(jobId, new HashMap<>());
        }
        if (startedStatusMap.get(jobId).containsKey(taskId)) {
            // Job did not complete, new job started
            jobRunsById.get(jobId).add(CompactionJobRun.started(taskId, startedStatusMap.get(jobId).get(taskId)));
        }
        startedStatusMap.get(jobId).put(taskId, statusUpdate);
        return this;
    }

    public CompactionJobStatusesBuilder jobFinished(
            String jobId, CompactionJobFinishedStatus statusUpdate, String taskId) {
        CompactionJobStartedStatus startedStatus = null;
        if (startedStatusMap.containsKey(jobId)) {
            startedStatus = startedStatusMap.get(jobId).get(taskId);
        }
        jobRunsById.get(jobId).add(CompactionJobRun.finished(taskId, startedStatus, statusUpdate));
        return this;
    }

    public Stream<CompactionJobStatus> stream() {
        return createdById.entrySet().stream()
                .map(entry -> fullStatus(entry.getKey(), entry.getValue()));
    }

    public CompactionJobStatusesBuilder expiryDate(
            String jobId, Instant expiryDate) {
        expiryDateById.put(jobId, expiryDate);
        return this;
    }

    public List<CompactionJobStatus> build() {
        return stream().collect(Collectors.toList());
    }

    private CompactionJobStatus fullStatus(String jobId, CompactionJobCreatedStatus created) {
        return CompactionJobStatus.builder().jobId(jobId)
                .createdStatus(created)
                .jobRuns(jobRunsById.get(jobId))
                .expiryDate(expiryDateById.get(jobId))
                .build();
    }
}
