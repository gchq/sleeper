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
package sleeper.clients.status.report.ingest.task;

import sleeper.core.tracker.ingest.task.IngestTaskStatus;
import sleeper.core.tracker.ingest.task.IngestTaskStatusStore;

import java.time.Instant;
import java.util.List;

@FunctionalInterface
public interface IngestTaskQuery {
    IngestTaskQuery ALL = IngestTaskStatusStore::getAllTasks;
    IngestTaskQuery UNFINISHED = IngestTaskStatusStore::getTasksInProgress;

    List<IngestTaskStatus> run(IngestTaskStatusStore store);

    static IngestTaskQuery from(String type) {
        switch (type) {
            case "-a":
                return ALL;
            case "-u":
                return UNFINISHED;
            default:
                throw new IllegalArgumentException("Unrecognised query type: " + type);
        }
    }

    static IngestTaskQuery forPeriod(Instant startTime, Instant endTime) {
        return store -> store.getTasksInTimePeriod(startTime, endTime);
    }
}
