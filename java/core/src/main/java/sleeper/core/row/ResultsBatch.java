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
package sleeper.core.row;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import sleeper.core.schema.Schema;

import java.util.List;

/**
 * A data structure for a batch of results from a query.
 */
public class ResultsBatch {
    private final String queryId;
    private final Schema schema;
    private final List<Row> rows;

    public ResultsBatch(String queryId, Schema schema, List<Row> rows) {
        this.queryId = queryId;
        this.schema = schema;
        this.rows = rows;
    }

    public String getQueryId() {
        return queryId;
    }

    public Schema getSchema() {
        return schema;
    }

    public List<Row> getRows() {
        return rows;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResultsBatch that = (ResultsBatch) o;

        return new EqualsBuilder()
                .append(queryId, that.queryId)
                .append(schema, that.schema)
                .append(rows, that.rows)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(13, 53)
                .append(queryId)
                .append(schema)
                .append(rows)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("queryId", queryId)
                .append("schema", schema)
                .append("rows", rows)
                .toString();
    }
}
