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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompactionJobStatusesBuilder {

    private final Map<String, CompactionJobCreatedStatus> createdById = new TreeMap<>(); // Order by job ID for output
    private final Map<String, CompactionJobStatusUpdateRecord> createdUpdateByJobId = new HashMap<>();
    private final Map<String, List<CompactionJobStatusUpdateRecord>> runUpdatesByJobId = new HashMap<>();
    private final Map<String, List<CompactionJobRun>> jobRunsById = new HashMap<>();
    private final Map<String, Instant> expiryDateById = new HashMap<>();
    private final Map<String, Map<String, CompactionJobStartedStatus>> startedStatusMap = new HashMap<>();

    public CompactionJobStatusesBuilder jobUpdates(List<CompactionJobStatusUpdateRecord> jobUpdates) {
        jobUpdates.forEach(this::jobUpdate);
        return this;
    }

    public CompactionJobStatusesBuilder jobUpdate(CompactionJobStatusUpdateRecord jobUpdate) {
        if (jobUpdate.getStatusUpdate() instanceof CompactionJobCreatedStatus) {
            createdUpdateByJobId.put(jobUpdate.getJobId(), jobUpdate);
        } else {
            runUpdatesByJobId.computeIfAbsent(jobUpdate.getJobId(), id -> new ArrayList<>())
                    .add(jobUpdate);
        }
        return this;
    }

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
            jobRunsById.get(jobId).add(CompactionJobRun.started(taskId, startedStatusMap.get(jobId).remove(taskId)));
        }
        startedStatusMap.get(jobId).put(taskId, statusUpdate);
        return this;
    }

    public CompactionJobStatusesBuilder jobFinished(
            String jobId, CompactionJobFinishedStatus statusUpdate, String taskId) {
        if (startedStatusMap.containsKey(jobId) && startedStatusMap.get(jobId).containsKey(taskId)) {
            jobRunsById.get(jobId).add(CompactionJobRun.finished(taskId, startedStatusMap.get(jobId).remove(taskId), statusUpdate));
        }
        return this;
    }

    public Stream<CompactionJobStatus> stream() {
        return createdUpdateByJobId.entrySet().stream()
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

    private CompactionJobStatus fullStatus(String jobId, CompactionJobStatusUpdateRecord createdUpdate) {
        CompactionJobCreatedStatus createdStatus = (CompactionJobCreatedStatus) createdUpdate.getStatusUpdate();
        List<CompactionJobStatusUpdateRecord> runUpdates = runUpdatesOrderedByUpdateTime(jobId);

        return CompactionJobStatus.builder().jobId(jobId)
                .createdStatus(createdStatus)
                .jobRuns(buildJobRunList(runUpdates))
                .expiryDate(last(runUpdates)
                        .map(CompactionJobStatusUpdateRecord::getExpiryDate)
                        .orElseGet(createdUpdate::getExpiryDate))
                .build();
    }

    public List<CompactionJobRun> buildJobRunList(List<CompactionJobStatusUpdateRecord> recordList) {
        boolean previousStarted = false;
        List<CompactionJobRun> jobRuns = new ArrayList<>();
        CompactionJobRun.Builder runBuilder = CompactionJobRun.builder();
        for (CompactionJobStatusUpdateRecord record : recordList) {
            CompactionJobStatusUpdate statusUpdate = record.getStatusUpdate();
            if (statusUpdate instanceof CompactionJobStartedStatus) {
                if (previousStarted) {
                    jobRuns.add(runBuilder.build());
                    runBuilder = CompactionJobRun.builder();
                }
                runBuilder.startedStatus((CompactionJobStartedStatus) statusUpdate)
                        .taskId(record.getTaskId());
                previousStarted = true;
            } else {
                jobRuns.add(runBuilder.finishedStatus((CompactionJobFinishedStatus) statusUpdate)
                        .taskId(record.getTaskId()).build());
                runBuilder = CompactionJobRun.builder();
                previousStarted = false;
            }
        }
        if (previousStarted) {
            jobRuns.add(runBuilder.build());
        }
        return jobRuns;
    }

    private List<CompactionJobStatusUpdateRecord> runUpdatesOrderedByUpdateTime(String jobId) {
        return runUpdatesByJobId.getOrDefault(jobId, Collections.emptyList())
                .stream().sorted(Comparator.comparing(update -> update.getStatusUpdate().getUpdateTime()))
                .collect(Collectors.toList());
    }

    private static <T> Optional<T> last(List<T> list) {
        if (list.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(list.get(list.size() - 1));
        }
    }
}
