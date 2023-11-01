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

package sleeper.query.model;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.query.model.output.ResultsOutputInfo;
import sleeper.query.tracker.QueryStatusReportListener;

import java.util.Objects;

public class QueryOrLeafQuery {

    private final QueryNew query;
    private final LeafPartitionQuery leafQuery;

    public QueryOrLeafQuery(QueryNew query) {
        this.query = Objects.requireNonNull(query, "query must not be null");
        this.leafQuery = null;
    }

    public QueryOrLeafQuery(LeafPartitionQuery leafQuery) {
        this.query = null;
        this.leafQuery = Objects.requireNonNull(leafQuery, "leafQuery must not be null");
    }

    public boolean isLeafQuery() {
        return leafQuery != null;
    }

    public QueryNew asParentQuery() {
        return Objects.requireNonNull(query, "query is a leaf query");
    }

    public LeafPartitionQuery asLeafQuery() {
        return Objects.requireNonNull(leafQuery, "query is not a leaf query");
    }

    public void reportCompleted(QueryStatusReportListener listener, ResultsOutputInfo outputInfo) {
        if (leafQuery != null) {
            listener.queryCompleted(leafQuery, outputInfo);
        } else {
            listener.queryCompleted(query.toOld(), outputInfo);
        }
    }

    public void reportFailed(QueryStatusReportListener listener, Exception e) {
        if (leafQuery != null) {
            listener.queryFailed(leafQuery, e);
        } else {
            listener.queryFailed(query.toOld(), e);
        }
    }

    public String getQueryId() {
        if (leafQuery != null) {
            return leafQuery.getQueryId();
        } else {
            return query.getQueryId();
        }
    }

    public TableProperties getTableProperties(TablePropertiesProvider provider) {
        if (leafQuery != null) {
            return provider.getByName(leafQuery.getTableName());
        } else {
            return provider.getByName(query.getTableName());
        }
    }

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
        QueryOrLeafQuery that = (QueryOrLeafQuery) object;
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
