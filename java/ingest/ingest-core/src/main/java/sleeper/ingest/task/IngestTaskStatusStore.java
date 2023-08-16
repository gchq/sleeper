/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.ingest.task;

import java.time.Instant;
import java.util.List;

public interface IngestTaskStatusStore {

    IngestTaskStatusStore NONE = new IngestTaskStatusStore() {
    };

    default void taskStarted(IngestTaskStatus taskStatus) {
    }

    default void taskFinished(IngestTaskStatus taskStatus) {
    }

    default IngestTaskStatus getTask(String taskId) {
        throw new UnsupportedOperationException("Instance has no ingest task status store");
    }

    default List<IngestTaskStatus> getAllTasks() {
        throw new UnsupportedOperationException("Instance has no ingest task status store");
    }

    default List<IngestTaskStatus> getTasksInTimePeriod(Instant startTime, Instant endTime) {
        throw new UnsupportedOperationException("Instance has no ingest task status store");
    }

    default List<IngestTaskStatus> getTasksInProgress() {
        throw new UnsupportedOperationException("Instance has no ingest task status store");
    }
}
