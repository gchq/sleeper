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

package sleeper.compaction.core.testutils;

import sleeper.compaction.core.task.CompactionTaskStatusStore;
import sleeper.core.tracker.compaction.task.CompactionTaskStatus;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class InMemoryCompactionTaskStatusStore implements CompactionTaskStatusStore {
    private final Map<String, CompactionTaskStatus> statusByTaskId = new LinkedHashMap<>();

    @Override
    public void taskStarted(CompactionTaskStatus taskStatus) {
        if (taskStatus.isFinished()) {
            throw new IllegalStateException("Task finished before reported as started: " + taskStatus.getTaskId());
        }
        if (statusByTaskId.containsKey(taskStatus.getTaskId())) {
            throw new IllegalStateException("Task already started: " + taskStatus.getTaskId());
        }
        statusByTaskId.put(taskStatus.getTaskId(), taskStatus);
    }

    @Override
    public void taskFinished(CompactionTaskStatus taskStatus) {
        if (!statusByTaskId.containsKey(taskStatus.getTaskId())) {
            throw new IllegalStateException("Task not started: " + taskStatus.getTaskId());
        }
        statusByTaskId.put(taskStatus.getTaskId(), taskStatus);
    }

    public void taskStartedAndFinished(CompactionTaskStatus taskStatus) {
        if (statusByTaskId.containsKey(taskStatus.getTaskId())) {
            throw new IllegalStateException("Task already started: " + taskStatus.getTaskId());
        }
        statusByTaskId.put(taskStatus.getTaskId(), taskStatus);
    }

    @Override
    public List<CompactionTaskStatus> getAllTasks() {
        return reverse(statusByTaskId.values().stream());
    }

    @Override
    public CompactionTaskStatus getTask(String taskId) {
        return statusByTaskId.get(taskId);
    }

    @Override
    public List<CompactionTaskStatus> getTasksInTimePeriod(Instant startTime, Instant endTime) {
        return reverse(statusByTaskId.values().stream()
                .filter(task -> task.isInPeriod(startTime, endTime)));
    }

    @Override
    public List<CompactionTaskStatus> getTasksInProgress() {
        return reverse(statusByTaskId.values().stream()
                .filter(task -> !task.isFinished()));
    }

    private static List<CompactionTaskStatus> reverse(Stream<CompactionTaskStatus> tasks) {
        LinkedList<CompactionTaskStatus> list = new LinkedList<>();
        tasks.forEach(list::push);
        return list;
    }
}
