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
package sleeper.core.tracker.ingest.task;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * An in-memory ingest task status store backed by a map.
 */
public class InMemoryIngestTaskStatusStore implements IngestTaskStatusStore {

    private final Map<String, IngestTaskStatus> statusByTaskId = new LinkedHashMap<>();

    @Override
    public void taskStarted(IngestTaskStatus taskStatus) {
        if (taskStatus.isFinished()) {
            throw new IllegalStateException("Task finished before reported as started: " + taskStatus.getTaskId());
        }
        if (statusByTaskId.containsKey(taskStatus.getTaskId())) {
            throw new IllegalStateException("Task already started: " + taskStatus.getTaskId());
        }
        statusByTaskId.put(taskStatus.getTaskId(), taskStatus);
    }

    @Override
    public void taskFinished(IngestTaskStatus taskStatus) {
        if (!statusByTaskId.containsKey(taskStatus.getTaskId())) {
            throw new IllegalStateException("Task not started: " + taskStatus.getTaskId());
        }
        statusByTaskId.put(taskStatus.getTaskId(), taskStatus);
    }

    @Override
    public List<IngestTaskStatus> getAllTasks() {
        return new ArrayList<>(statusByTaskId.values());
    }

    @Override
    public List<IngestTaskStatus> getTasksInProgress() {
        return statusByTaskId.values().stream()
                .filter(not(IngestTaskStatus::isFinished))
                .collect(toUnmodifiableList());
    }
}
