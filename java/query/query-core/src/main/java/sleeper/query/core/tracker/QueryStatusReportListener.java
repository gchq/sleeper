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
 * Interface for reporting on the status of a query.
 */
public interface QueryStatusReportListener {
    String DESTINATION = "destination";

    /**
     * Check if the query has been queued.
     *
     * @param query the Sleeper query
     */
    void queryQueued(Query query);

    /**
     * Check if the query is still in progress.
     *
     * @param query the Sleeper query
     */
    void queryInProgress(Query query);

    /**
     * Check if the leaf partition query is still in progress.
     *
     * @param leafQuery the Sleeper leaf partition query
     */
    void queryInProgress(LeafPartitionQuery leafQuery);

    /**
     * Check if any sub queries have been created.
     *
     * @param query      the Sleeper query
     * @param subQueries the sub queries
     */
    void subQueriesCreated(Query query, List<LeafPartitionQuery> subQueries);

    /**
     * Check if the query has completed.
     *
     * @param query      the Sleeper query
     * @param outputInfo the query results output information
     */
    void queryCompleted(Query query, ResultsOutputInfo outputInfo);

    /**
     * Check if the leaf partition query has completed.
     *
     * @param leafQuery  the Sleeper leaf partition query
     * @param outputInfo the query results output information
     */
    void queryCompleted(LeafPartitionQuery leafQuery, ResultsOutputInfo outputInfo);

    /**
     * Check if the query has failed.
     *
     * @param query the Sleeper query
     * @param e     the exception raised
     */
    void queryFailed(Query query, Exception e);

    /**
     * Check if the query has failed from the query Id.
     *
     * @param queryId the query Id
     * @param e       the exception raised
     */
    void queryFailed(String queryId, Exception e);

    /**
     * Check if the leaf partition query has failed.
     *
     * @param leafQuery the Sleeper leaf partition query
     * @param e         the exception raised
     */
    void queryFailed(LeafPartitionQuery leafQuery, Exception e);

}
