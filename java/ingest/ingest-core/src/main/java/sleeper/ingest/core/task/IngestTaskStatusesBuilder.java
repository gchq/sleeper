/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.ingest.core.task;

import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A helper for constructing multiple ingest task statuses.
 */
public class IngestTaskStatusesBuilder {
    private final Map<String, IngestTaskStatus.Builder> builderById = new HashMap<>();

    /**
     * Updates the ingest task status builder with start time and an expiry date.
     *
     * @param  taskId     the ingest task ID
     * @param  startTime  the start time
     * @param  expiryDate the expiry date
     * @return            this class for chaining
     */
    public IngestTaskStatusesBuilder taskStarted(
            String taskId, Instant startTime, Instant expiryDate) {
        builderById.computeIfAbsent(taskId,
                id -> IngestTaskStatus.builder().taskId(id))
                .startTime(startTime).expiryDate(expiryDate);
        return this;
    }

    /**
     * Updates the ingest task status builder with a task finished status.
     *
     * @param  taskId         the ingest task ID
     * @param  finishedStatus the task finished status
     * @return                this class for chaining
     */
    public IngestTaskStatusesBuilder taskFinished(
            String taskId, IngestTaskFinishedStatus finishedStatus) {
        Optional.ofNullable(builderById.get(taskId))
                .ifPresent(builder -> builder.finishedStatus(finishedStatus));
        return this;
    }

    /**
     * Stream all builders, building them and sorting them by oldest first.
     *
     * @return a stream of {@link IngestTaskStatus}, ordered by oldest first
     */
    public Stream<IngestTaskStatus> stream() {
        return builderById.values().stream()
                .map(IngestTaskStatus.Builder::build)
                .sorted(Comparator.comparing(IngestTaskStatus::getStartTime).reversed());
    }

    public List<IngestTaskStatus> build() {
        return stream().collect(Collectors.toList());
    }

}
