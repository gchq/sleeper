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

package sleeper.clients.report.query;

import sleeper.query.core.tracker.TrackedQuery;

import java.util.List;

/**
 * A reporter to present the status of queries as held in the query tracker. The format and output destination can vary
 * based on the implementation.
 */
public interface QueryTrackerReporter {

    /**
     * Writes a report on the status of queries.
     *
     * @param query   the query that was made against the tracker to retrieve the statuses
     * @param queries the status of queries retrieved from the tracker
     */
    void report(TrackerQuery query, List<TrackedQuery> queries);
}
