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

import sleeper.query.tracker.QueryStatusReportListeners;

import java.util.Objects;
import java.util.Optional;

public class QueryOrLeafQuery {

    private final Query query;
    private final SubQuery leafQuery;

    public QueryOrLeafQuery(Query query) {
        this.query = query;
        this.leafQuery = null;
    }

    public QueryOrLeafQuery(SubQuery leafQuery) {
        this.query = null;
        this.leafQuery = leafQuery;
    }

    public Optional<Query> getQuery() {
        return Optional.ofNullable(query);
    }

    public Optional<SubQuery> getLeafQuery() {
        return Optional.ofNullable(leafQuery);
    }

    public Query getParentQuery() {
        return getLeafQuery()
                .map(SubQuery::getParentQuery)
                .orElse(query);
    }

    public Query getThisQuery() {
        return getLeafQuery()
                .<Query>map(SubQuery::toLeafQuery)
                .orElse(query);
    }

    public void reportInProgress(QueryStatusReportListeners queryTrackers) {
        getLeafQuery().ifPresentOrElse(
                queryTrackers::queryInProgress,
                () -> queryTrackers.queryInProgress(query));
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
