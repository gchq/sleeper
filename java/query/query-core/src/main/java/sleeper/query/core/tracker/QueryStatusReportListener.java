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

import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.output.ResultsOutputInfo;

import java.util.List;

/**
 * This function tracks changes to the status of a query.
 * It is implemented by something that should be notified when the status changes for example
 * that includes the query tracker, and the web socket for a query that was made through a web socket.
 */
public interface QueryStatusReportListener {
    String DESTINATION = "destination";

    /**
     * Notifies when a query has been queued.
     *
     * @param query the Sleeper query
     */
    void queryQueued(Query query);

    /**
     * Notifies when a query is in progress.
     *
     * @param query the Sleeper query
     */
    void queryInProgress(Query query);

    /**
     * Notifies if a sub query is in progress.
     *
     * @param leafQuery the Sleeper leaf partition query
     */
    void queryInProgress(LeafPartitionQuery leafQuery);

    /**
     * Notifies if a sub query has been created.
     *
     * @param query      the Sleeper query
     * @param subQueries the sub queries
     */
    void subQueriesCreated(Query query, List<LeafPartitionQuery> subQueries);

    /**
     * Notifies when a query completes.
     *
     * @param query      the Sleeper query
     * @param outputInfo the query results output information
     */
    void queryCompleted(Query query, ResultsOutputInfo outputInfo);

    /**
     * Notifies when a sub query completes.
     *
     * @param leafQuery  the Sleeper leaf partition query
     * @param outputInfo the query results output information
     */
    void queryCompleted(LeafPartitionQuery leafQuery, ResultsOutputInfo outputInfo);

    /**
     * Notifies when a query fails.
     *
     * @param query the Sleeper query
     * @param e     the exception raised
     */
    void queryFailed(Query query, Exception e);

    /**
     * Notifies when a specific query fails.
     *
     * @param queryId the query ID
     * @param e       the exception raised
     */
    void queryFailed(String queryId, Exception e);

    /**
     * Notifies when a sub query fails.
     *
     * @param leafQuery the Sleeper leaf partition query
     * @param e         the exception raised
     */
    void queryFailed(LeafPartitionQuery leafQuery, Exception e);

}
