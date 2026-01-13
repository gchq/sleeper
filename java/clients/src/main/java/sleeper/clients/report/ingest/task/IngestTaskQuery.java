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
package sleeper.clients.report.ingest.task;

import sleeper.core.tracker.ingest.task.IngestTaskStatus;
import sleeper.core.tracker.ingest.task.IngestTaskTracker;

import java.time.Instant;
import java.util.List;

/**
 * A query to retreive details of ingest tasks held in the task tracker, to generate a report.
 */
@FunctionalInterface
public interface IngestTaskQuery {
    IngestTaskQuery ALL = IngestTaskTracker::getAllTasks;
    IngestTaskQuery UNFINISHED = IngestTaskTracker::getTasksInProgress;

    /**
     * Retrieves the data for the report.
     *
     * @param  tracker the task tracker
     * @return         the status of tasks covered by the query
     */
    List<IngestTaskStatus> run(IngestTaskTracker tracker);

    /**
     * Creates a query based on a type argument passed on the command line.
     *
     * @param  type the type string
     * @return      the query
     */
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

    /**
     * Creates a query for a given time period.
     *
     * @param  startTime the start of the time period
     * @param  endTime   the end of the time period
     * @return           the query
     */
    static IngestTaskQuery forPeriod(Instant startTime, Instant endTime) {
        return store -> store.getTasksInTimePeriod(startTime, endTime);
    }
}
