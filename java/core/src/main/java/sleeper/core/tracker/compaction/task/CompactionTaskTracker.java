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
import java.util.List;

/**
 * Tracks and reports on the status of compaction tasks. This stores events for each task. This is used for reporting.
 */
public interface CompactionTaskTracker {

    CompactionTaskTracker NONE = new CompactionTaskTracker() {
    };

    /**
     * Stores an event when a compaction task has started.
     *
     * @param taskStatus the status of the task
     */
    default void taskStarted(CompactionTaskStatus taskStatus) {
    }

    /**
     * Stores an event when a compaction task has finished.
     *
     * @param taskStatus the status of the task
     */
    default void taskFinished(CompactionTaskStatus taskStatus) {
    }

    /**
     * Retrieves the currently tracked status of a compaction task. This will be derived from events that have been
     * tracked for the task.
     *
     * @param  taskId                        the compaction task ID
     * @return                               the status of the task
     * @throws UnsupportedOperationException if the compaction task tracker is disabled for this Sleeper instance
     */
    default CompactionTaskStatus getTask(String taskId) {
        throw new UnsupportedOperationException("Instance has no compaction task tracker");
    }

    /**
     * Retrieves the status of all currently tracked compaction tasks. This includes all tasks that have tracked events,
     * unless those events have expired and are no longer held in the tracker.
     *
     * @return                               the task statuses
     * @throws UnsupportedOperationException if the compaction task tracker is disabled for this Sleeper instance
     */
    default List<CompactionTaskStatus> getAllTasks() {
        throw new UnsupportedOperationException("Instance has no compaction task tracker");
    }

    /**
     * Retrieves the status of compaction tasks that have events in or overlap with a given time period.
     *
     * @param  startTime                     the start of the period
     * @param  endTime                       the end of the period
     * @return                               the task statuses
     * @throws UnsupportedOperationException if the compaction task tracker is disabled for this Sleeper instance
     */
    default List<CompactionTaskStatus> getTasksInTimePeriod(Instant startTime, Instant endTime) {
        throw new UnsupportedOperationException("Instance has no compaction task tracker");
    }

    /**
     * Retrieves the status of compaction tasks that are currently in progress.
     *
     * @return                               the task statuses
     * @throws UnsupportedOperationException if the compaction task tracker is disabled for this Sleeper instance
     */
    default List<CompactionTaskStatus> getTasksInProgress() {
        throw new UnsupportedOperationException("Instance has no compaction task tracker");
    }
}
