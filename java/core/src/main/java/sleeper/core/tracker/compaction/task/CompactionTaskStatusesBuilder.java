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
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class is used to manage task status builders in a cache.
 * Tasks can be started, finished or streamed.
 */
public class CompactionTaskStatusesBuilder {
    private final Map<String, CompactionTaskStatus.Builder> builderById = new HashMap<>();

    /**
     * This method checks for a cached CompactionTaskStatus Builder and returns it if found or creates a new one if not
     * found.
     *
     * @param  taskId     The String task Id for the task to be started.
     * @param  startTime  The Instant time the task should be started from.
     * @param  expiryDate The Instant Date the task should expire on.
     * @return            CompactionTaskSatusesBuilder with the started task.
     */
    public CompactionTaskStatusesBuilder taskStarted(
            String taskId, Instant startTime, Instant expiryDate) {
        builderById.computeIfAbsent(taskId,
                id -> CompactionTaskStatus.builder().taskId(id))
                .startTime(startTime).expiryDate(expiryDate);
        return this;
    }

    /**
     * This method finishes a task with the supplied status provided it exists in the cached map.
     *
     * @param  taskId         The String task Id of the task to be finished
     * @param  finishedStatus The CompactionTaskFinishedStatus detailing the specific finishing status
     * @return                CompactionTaskSatusesBuilder with the finished task.
     */
    public CompactionTaskStatusesBuilder taskFinished(
            String taskId, CompactionTaskFinishedStatus finishedStatus) {
        Optional.ofNullable(builderById.get(taskId))
                .ifPresent(builder -> builder.finishedStatus(finishedStatus));
        return this;
    }

    /**
     * This method returns a stream of the CompacionJobStatuses having been built and ordered by reversed start time.
     *
     * @return Stream of CompactionTrackStatus having been built and ordered by reversed start time.
     */
    public Stream<CompactionTaskStatus> stream() {
        return builderById.values().stream()
                .map(CompactionTaskStatus.Builder::build)
                .sorted(Comparator.comparing(CompactionTaskStatus::getStartTime).reversed());
    }

    public List<CompactionTaskStatus> build() {
        return stream().collect(Collectors.toList());
    }

}
