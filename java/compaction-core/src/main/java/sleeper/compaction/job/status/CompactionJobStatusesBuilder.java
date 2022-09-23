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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CompactionJobStatusesBuilder {

    private final Map<String, CompactionJobCreatedStatus> createdById = new TreeMap<>(); // Order by job ID for output
    private final Map<String, CompactionJobStartedStatus> startedById = new HashMap<>();
    private final Map<String, CompactionJobFinishedStatus> finishedById = new HashMap<>();

    public CompactionJobStatusesBuilder jobCreated(
            String jobId, CompactionJobCreatedStatus statusUpdate) {
        createdById.put(jobId, statusUpdate);
        return this;
    }

    public CompactionJobStatusesBuilder jobStarted(
            String jobId, CompactionJobStartedStatus statusUpdate) {
        startedById.put(jobId, statusUpdate);
        return this;
    }

    public CompactionJobStatusesBuilder jobFinished(
            String jobId, CompactionJobFinishedStatus statusUpdate) {
        finishedById.put(jobId, statusUpdate);
        return this;
    }

    public List<CompactionJobStatus> build() {
        return createdById.entrySet().stream()
                .map(entry -> fullStatus(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    private CompactionJobStatus fullStatus(String jobId, CompactionJobCreatedStatus created) {
        return CompactionJobStatus.builder().jobId(jobId)
                .createdStatus(created)
                .startedStatus(startedById.get(jobId))
                .finishedStatus(finishedById.get(jobId))
                .build();
    }
}
