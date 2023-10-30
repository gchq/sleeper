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

import java.util.Objects;
import java.util.Optional;

public class QueryOrLeafQuery {

    private final Query query;
    private final SubQuery subQuery;

    public QueryOrLeafQuery(Query query) {
        this.query = query;
        this.subQuery = null;
    }

    public QueryOrLeafQuery(SubQuery subQuery) {
        this.query = null;
        this.subQuery = subQuery;
    }

    public Optional<Query> getQuery() {
        return Optional.ofNullable(query);
    }

    public Optional<SubQuery> getSubQuery() {
        return Optional.ofNullable(subQuery);
    }

    public Query getParentQuery() {
        return getSubQuery()
                .map(SubQuery::getParentQuery)
                .orElse(query);
    }

    public Query getThisQuery() {
        return getSubQuery()
                .<Query>map(SubQuery::toLeafQuery)
                .orElse(query);
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
        return Objects.equals(query, that.query) && Objects.equals(subQuery, that.subQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, subQuery);
    }

    @Override
    public String toString() {
        if (subQuery != null) {
            return subQuery.toString();
        } else {
            return String.valueOf(query);
        }
    }
}
