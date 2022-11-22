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

import sleeper.core.status.ProcessStatusUpdate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompactionJobStatusesBuilder {
    private final Map<String, CompactionJobStatusUpdateRecord> createdUpdateByJobId = new HashMap<>();
    private final Map<String, List<CompactionJobStatusUpdateRecord>> runUpdatesByJobId = new HashMap<>();

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

    public Stream<CompactionJobStatus> stream() {
        return createdUpdateByJobId.entrySet().stream()
                .sorted(Comparator.comparing(
                        (Map.Entry<String, CompactionJobStatusUpdateRecord> update) ->
                                update.getValue().getStatusUpdate().getUpdateTime()).reversed())
                .map(entry -> fullStatus(entry.getKey(), entry.getValue()));
    }

    public List<CompactionJobStatus> build() {
        return stream().collect(Collectors.toList());
    }

    private CompactionJobStatus fullStatus(String jobId, CompactionJobStatusUpdateRecord createdUpdate) {
        CompactionJobCreatedStatus createdStatus = (CompactionJobCreatedStatus) createdUpdate.getStatusUpdate();
        List<CompactionJobStatusUpdateRecord> runUpdates = runUpdatesOrderedByUpdateTime(jobId);

        return CompactionJobStatus.builder().jobId(jobId)
                .createdStatus(createdStatus)
                .jobRunsLatestFirst(buildJobRunList(runUpdates))
                .expiryDate(last(runUpdates)
                        .map(CompactionJobStatusUpdateRecord::getExpiryDate)
                        .orElseGet(createdUpdate::getExpiryDate))
                .build();
    }

    public List<CompactionJobRun> buildJobRunList(List<CompactionJobStatusUpdateRecord> recordList) {
        Map<String, CompactionJobRun.Builder> taskBuilders = new HashMap<>();
        List<CompactionJobRun.Builder> orderedBuilders = new ArrayList<>();
        for (CompactionJobStatusUpdateRecord record : recordList) {
            String taskId = record.getTaskId();
            ProcessStatusUpdate statusUpdate = record.getStatusUpdate();
            if (statusUpdate instanceof CompactionJobStartedStatus) {
                CompactionJobRun.Builder builder = CompactionJobRun.builder()
                        .startedStatus((CompactionJobStartedStatus) statusUpdate)
                        .taskId(taskId);
                taskBuilders.put(taskId, builder);
                orderedBuilders.add(builder);
            } else {
                taskBuilders.remove(taskId)
                        .finishedStatus((CompactionJobFinishedStatus) statusUpdate)
                        .taskId(taskId);
            }
        }
        List<CompactionJobRun> jobRuns = orderedBuilders.stream()
                .map(CompactionJobRun.Builder::build)
                .collect(Collectors.toList());
        Collections.reverse(jobRuns);
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
