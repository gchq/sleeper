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

package sleeper.query.core.tracker;

import java.util.List;

/**
 * Interface for tracking the status of a query.
 */
public interface QueryTrackerStore {
    /**
     * Get the status of a query by query Id.
     *
     * @param  queryId               the query Id
     * @return                       the tracked query
     * @throws QueryTrackerException if something goes wrong checking the status of the query
     */
    TrackedQuery getStatus(String queryId) throws QueryTrackerException;

    /**
     * Get the status of a specific query and sub query.
     *
     * @param  queryId               the query Id
     * @param  subQueryId            the sub query Id
     * @return                       the tracked query
     * @throws QueryTrackerException if something goes wrong checking the status of the query and sub query
     */
    TrackedQuery getStatus(String queryId, String subQueryId) throws QueryTrackerException;

    /**
     * Get all queries.
     *
     * @return a list of all tracked queries
     */
    List<TrackedQuery> getAllQueries();

    /**
     * Get any queries that are in a specific state.
     *
     * @param  state the query state
     * @return       a list of tracked queries in the supplied state
     */
    List<TrackedQuery> getQueriesWithState(QueryState state);

    /**
     * Get any a failed querues.
     *
     * @return a list of tracked queries that have failed.
     */
    List<TrackedQuery> getFailedQueries();
}
