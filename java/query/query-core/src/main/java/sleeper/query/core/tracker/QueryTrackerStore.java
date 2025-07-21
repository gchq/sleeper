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
 * Query tracker store.
 */
public interface QueryTrackerStore {
    /**
     * Retrieves the status of a query by query ID.
     *
     * @param  queryId               the query ID
     * @return                       the tracked query
     * @throws QueryTrackerException if something goes wrong checking the status of the query
     */
    TrackedQuery getStatus(String queryId) throws QueryTrackerException;

    /**
     * Retrieves the status of a specific sub query.
     *
     * @param  queryId               the query ID
     * @param  subQueryId            the sub query ID
     * @return                       the tracked query
     * @throws QueryTrackerException if something goes wrong checking the status of the sub query
     */
    TrackedQuery getStatus(String queryId, String subQueryId) throws QueryTrackerException;

    /**
     * Retrieves all queries.
     *
     * @return a list of all tracked queries
     */
    List<TrackedQuery> getAllQueries();

    /**
     * Retrieves any queries that are in a specific state.
     *
     * @param  state the query state
     * @return       a list of tracked queries in the supplied state
     */
    List<TrackedQuery> getQueriesWithState(QueryState state);

    /**
     * Retrieves failed queries.
     *
     * @return a list of tracked queries that have failed.
     */
    List<TrackedQuery> getFailedQueries();
}
