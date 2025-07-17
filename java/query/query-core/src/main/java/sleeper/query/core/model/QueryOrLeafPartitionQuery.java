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

package sleeper.query.core.model;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.tracker.QueryStatusReportListener;

import java.util.Objects;

/**
 * Deserializes a query or sub-query from JSON.
 */
public class QueryOrLeafPartitionQuery {

    private final Query query;
    private final LeafPartitionQuery leafQuery;

    public QueryOrLeafPartitionQuery(Query query) {
        this.query = Objects.requireNonNull(query, "query must not be null");
        this.leafQuery = null;
    }

    public QueryOrLeafPartitionQuery(LeafPartitionQuery leafQuery) {
        this.query = null;
        this.leafQuery = Objects.requireNonNull(leafQuery, "leafQuery must not be null");
    }

    public boolean isLeafQuery() {
        return leafQuery != null;
    }

    /**
     * Return a Sleeper query.
     *
     * @return a Sleeper query
     */
    public Query asParentQuery() {
        return Objects.requireNonNull(query, "query is a leaf query");
    }

    /**
     * Return a sub query.
     *
     * @return a leaf partition query
     */
    public LeafPartitionQuery asLeafQuery() {
        return Objects.requireNonNull(leafQuery, "query is not a leaf query");
    }

    /**
     * If the query has completed this method is called to publish the completed status.
     *
     * @param listener   listener to publish status of the query to
     * @param outputInfo information about the query results
     */
    public void reportCompleted(QueryStatusReportListener listener, ResultsOutputInfo outputInfo) {
        if (leafQuery != null) {
            listener.queryCompleted(leafQuery, outputInfo);
        } else {
            listener.queryCompleted(query, outputInfo);
        }
    }

    /**
     * If the query has failed this method is called to publish the failed status.
     *
     * @param listener listener to publish the status of the query to
     * @param e        exception raised during query processing
     */
    public void reportFailed(QueryStatusReportListener listener, Exception e) {
        if (leafQuery != null) {
            listener.queryFailed(leafQuery, e);
        } else {
            listener.queryFailed(query, e);
        }
    }

    /**
     * Return the Id of the query. A sub query will return the ID of the parent query.
     *
     * @return the query ID
     */
    public String getQueryId() {
        if (leafQuery != null) {
            return leafQuery.getQueryId();
        } else {
            return query.getQueryId();
        }
    }

    /**
     * Return the table properties.
     *
     * @param  provider cache of Sleeper table properties
     * @return          the table properties
     */
    public TableProperties getTableProperties(TablePropertiesProvider provider) {
        if (leafQuery != null) {
            return provider.getById(leafQuery.getTableId());
        } else {
            return provider.getByName(query.getTableName());
        }
    }

    /**
     * Return the query processing config.
     *
     * @return the query processing config
     */
    public QueryProcessingConfig getProcessingConfig() {
        if (leafQuery != null) {
            return leafQuery.getProcessingConfig();
        } else {
            return query.getProcessingConfig();
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        QueryOrLeafPartitionQuery that = (QueryOrLeafPartitionQuery) object;
        return Objects.equals(query, that.query) && Objects.equals(leafQuery, that.leafQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, leafQuery);
    }

    @Override
    public String toString() {
        if (leafQuery != null) {
            return leafQuery.toString();
        } else {
            return String.valueOf(query);
        }
    }
}
