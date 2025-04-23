/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.core.tracker.compaction.task;

import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * A builder to load compaction task statuses based on events stored in the tracker. Events can be added one at a time.
 * Once all events are in the builder, compaction task statuses can be retrieved.
 */
public class CompactionTaskStatusesBuilder {
    private final Map<String, CompactionTaskStatus.Builder> builderById = new HashMap<>();

    /**
     * Adds an event for when a compaction task started.
     *
     * @param  taskId     the ID of the task
     * @param  startTime  the time that the task started
     * @param  expiryDate the time the event will expire
     * @return            this builder
     */
    public CompactionTaskStatusesBuilder taskStarted(
            String taskId, Instant startTime, Instant expiryDate) {
        builderById.computeIfAbsent(taskId,
                id -> CompactionTaskStatus.builder().taskId(id))
                .startTime(startTime).expiryDate(expiryDate);
        return this;
    }

    /**
     * Adds an event for when a compaction task finished.
     *
     * @param  taskId         the ID of the task
     * @param  finishedStatus the status update
     * @return                this builder
     */
    public CompactionTaskStatusesBuilder taskFinished(
            String taskId, CompactionTaskFinishedStatus finishedStatus) {
        Optional.ofNullable(builderById.get(taskId))
                .ifPresent(builder -> builder.finishedStatus(finishedStatus));
        return this;
    }

    /**
     * Builds compaction task statuses from the provided events, and sorts them with the most recently started task
     * first.
     *
     * @return a stream of the compaction task statuses
     */
    public Stream<CompactionTaskStatus> stream() {
        return builderById.values().stream()
                .map(CompactionTaskStatus.Builder::build)
                .sorted(Comparator.comparing(CompactionTaskStatus::getStartTime).reversed());
    }

    /**
     * Builds compaction task statuses from the provided events, and sorts them with the most recently started task
     * first.
     *
     * @return a list of the compaction task statuses
     */
    public List<CompactionTaskStatus> build() {
        return stream().toList();
    }

}
