/*
 * Copyright 2022 Crown Copyright
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
package sleeper.query.tracker;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * A TrackedQuery contains information about a query including its id and current status.
 */
public class TrackedQuery {
    private final String queryId;
    private final String subQueryId;
    private final Long lastUpdateTime;
    private final Long expiryDate;
    private final QueryState lastKnownState;
    private final Long recordCount;

    public TrackedQuery(String queryId, String subQueryId, Long lastUpdateTime, Long expiryDate, QueryState lastKnownState, Long recordCount) {
        this.queryId = queryId;
        this.subQueryId = subQueryId;
        this.lastUpdateTime = lastUpdateTime;
        this.lastKnownState = lastKnownState;
        this.expiryDate = expiryDate;
        this.recordCount = recordCount;
    }

    public String getQueryId() {
        return queryId;
    }

    public QueryState getLastKnownState() {
        return lastKnownState;
    }

    public Long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public Long getExpiryDate() {
        return expiryDate;
    }

    public String getSubQueryId() {
        return subQueryId;
    }

    public Long getRecordCount() {
        return recordCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TrackedQuery that = (TrackedQuery) o;

        return new EqualsBuilder()
                .append(queryId, that.queryId)
                .append(subQueryId, that.subQueryId)
                .append(lastUpdateTime, that.lastUpdateTime)
                .append(expiryDate, that.expiryDate)
                .append(lastKnownState, that.lastKnownState)
                .append(recordCount, that.recordCount)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(queryId)
                .append(subQueryId)
                .append(lastUpdateTime)
                .append(expiryDate)
                .append(lastKnownState)
                .append(recordCount)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
                .append("queryId", queryId)
                .append("subQueryId", subQueryId)
                .append("lastUpdateTime", lastUpdateTime)
                .append("expiryDate", expiryDate)
                .append("lastKnownState", lastKnownState)
                .append("recordCount", recordCount)
                .toString();
    }
}
