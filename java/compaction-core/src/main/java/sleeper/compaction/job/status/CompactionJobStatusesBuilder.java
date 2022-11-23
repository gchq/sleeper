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

import sleeper.core.status.ProcessStatusUpdateRecord;
import sleeper.core.status.ProcessStatusesBuilder;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompactionJobStatusesBuilder {
    private final Map<String, ProcessStatusUpdateRecord> createdUpdateByJobId = new HashMap<>();
    private final ProcessStatusesBuilder processStatusesBuilder = new ProcessStatusesBuilder();

    public CompactionJobStatusesBuilder jobUpdates(List<ProcessStatusUpdateRecord> jobUpdates) {
        jobUpdates.forEach(this::jobUpdate);
        return this;
    }

    public CompactionJobStatusesBuilder jobUpdate(ProcessStatusUpdateRecord jobUpdate) {
        if (jobUpdate.getStatusUpdate() instanceof CompactionJobCreatedStatus) {
            createdUpdateByJobId.put(jobUpdate.getJobId(), jobUpdate);
        } else {
            processStatusesBuilder.processUpdate(jobUpdate);
        }
        return this;
    }

    public Stream<CompactionJobStatus> stream() {
        return createdUpdateByJobId.entrySet().stream()
                .sorted(Comparator.comparing(
                        (Map.Entry<String, ProcessStatusUpdateRecord> update) ->
                                update.getValue().getStatusUpdate().getUpdateTime()).reversed())
                .map(entry -> fullStatus(entry.getKey(), entry.getValue()));
    }

    public List<CompactionJobStatus> build() {
        return stream().collect(Collectors.toList());
    }

    private CompactionJobStatus fullStatus(String jobId, ProcessStatusUpdateRecord createdUpdate) {
        CompactionJobCreatedStatus createdStatus = (CompactionJobCreatedStatus) createdUpdate.getStatusUpdate();
        List<ProcessStatusUpdateRecord> runUpdates = processStatusesBuilder.runUpdatesOrderedByUpdateTime(jobId);

        return CompactionJobStatus.builder().jobId(jobId)
                .createdStatus(createdStatus)
                .jobRunsLatestFirst(processStatusesBuilder.buildRunList(runUpdates))
                .expiryDate(last(runUpdates)
                        .map(ProcessStatusUpdateRecord::getExpiryDate)
                        .orElseGet(createdUpdate::getExpiryDate))
                .build();
    }

    private static <T> Optional<T> last(List<T> list) {
        if (list.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(list.get(list.size() - 1));
        }
    }
}
