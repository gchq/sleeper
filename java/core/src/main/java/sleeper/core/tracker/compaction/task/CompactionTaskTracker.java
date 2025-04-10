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
 * This interface is used to track and update the status of tasks.
 */
public interface CompactionTaskTracker {

    CompactionTaskTracker NONE = new CompactionTaskTracker() {
    };

    /**
     * This method marks a task as a started with the provided status.
     *
     * @param taskStatus The CompactionTaskStatus the task should be started with.
     */
    default void taskStarted(CompactionTaskStatus taskStatus) {
    }

    /**
     * This method marks a task as a finished with the provided status.
     *
     * @param taskStatus The CompactionTaskStatus the task should be finished with.
     */
    default void taskFinished(CompactionTaskStatus taskStatus) {
    }

    /**
     * This method takes in a taskId and returns the current status of it.
     *
     * @param  taskId The taskId to get the status of.
     * @return        The current CompactionTaskStatus of the task.
     */
    default CompactionTaskStatus getTask(String taskId) {
        throw new UnsupportedOperationException("Instance has no compaction task tracker");
    }

    /**
     * This method gets all the tasks currently held.
     *
     * @return List of CompactionTaskStatuses.
     */
    default List<CompactionTaskStatus> getAllTasks() {
        throw new UnsupportedOperationException("Instance has no compaction task tracker");
    }

    /**
     * This method gets all the tasks currently held bettween a start and end time.
     *
     * @param  startTime The start time for the tasks to be returned from.
     * @param  endTime   The end time for the tasks to be returned from.
     * @return           List of CompactionTaskStatuses.
     */
    default List<CompactionTaskStatus> getTasksInTimePeriod(Instant startTime, Instant endTime) {
        throw new UnsupportedOperationException("Instance has no compaction task tracker");
    }

    /**
     * This method gets all the tasks that are currently in progress.
     *
     * @return List of CompactionTaskStatuses.
     */
    default List<CompactionTaskStatus> getTasksInProgress() {
        throw new UnsupportedOperationException("Instance has no compaction task tracker");
    }
}
