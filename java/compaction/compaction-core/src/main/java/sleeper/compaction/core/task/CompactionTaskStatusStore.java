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

package sleeper.compaction.core.task;

import sleeper.core.tracker.compaction.task.CompactionTaskStatus;

import java.time.Instant;
import java.util.List;

public interface CompactionTaskStatusStore {

    CompactionTaskStatusStore NONE = new CompactionTaskStatusStore() {
    };

    default void taskStarted(CompactionTaskStatus taskStatus) {
    }

    default void taskFinished(CompactionTaskStatus taskStatus) {
    }

    default CompactionTaskStatus getTask(String taskId) {
        throw new UnsupportedOperationException("Instance has no compaction task status store");
    }

    default List<CompactionTaskStatus> getAllTasks() {
        throw new UnsupportedOperationException("Instance has no compaction task status store");
    }

    default List<CompactionTaskStatus> getTasksInTimePeriod(Instant startTime, Instant endTime) {
        throw new UnsupportedOperationException("Instance has no compaction task status store");
    }

    default List<CompactionTaskStatus> getTasksInProgress() {
        throw new UnsupportedOperationException("Instance has no compaction task status store");
    }
}
