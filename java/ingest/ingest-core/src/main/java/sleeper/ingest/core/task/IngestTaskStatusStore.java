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
import java.util.List;

/**
 * Stores ingest task statuses.
 */
public interface IngestTaskStatusStore {

    IngestTaskStatusStore NONE = new IngestTaskStatusStore() {
    };

    /**
     * Saves an ingest task that has started.
     *
     * @param taskStatus the ingest task status
     */
    default void taskStarted(IngestTaskStatus taskStatus) {
    }

    /**
     * Saves an ingest task that has finished.
     *
     * @param taskStatus the ingest task status
     */
    default void taskFinished(IngestTaskStatus taskStatus) {
    }

    /**
     * Get the status of an ingest task by the task ID.
     *
     * @param  taskId the task ID
     * @return        the ingest task status
     */
    default IngestTaskStatus getTask(String taskId) {
        throw new UnsupportedOperationException("Instance has no ingest task status store");
    }

    /**
     * Gets the status of all ingest tasks.
     *
     * @return a list of all ingest task
     */
    default List<IngestTaskStatus> getAllTasks() {
        throw new UnsupportedOperationException("Instance has no ingest task status store");
    }

    /**
     * Gets the status of all tasks that have occurred within a time window.
     *
     * @param  startTime the time window start
     * @param  endTime   the time window end
     * @return           a list of all tasks that have occurred within a time window
     */
    default List<IngestTaskStatus> getTasksInTimePeriod(Instant startTime, Instant endTime) {
        throw new UnsupportedOperationException("Instance has no ingest task status store");
    }

    /**
     * Gets the status of all tasks that have not finished.
     *
     * @return a list of all tasks that have not finished
     */
    default List<IngestTaskStatus> getTasksInProgress() {
        throw new UnsupportedOperationException("Instance has no ingest task status store");
    }
}
