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

    private TrackedQuery(Builder builder) {
        queryId = builder.queryId;
        subQueryId = builder.subQueryId;
        lastUpdateTime = builder.lastUpdateTime;
        expiryDate = builder.expiryDate;
        lastKnownState = builder.lastKnownState;
        recordCount = builder.recordCount;
    }

    public static Builder builder() {
        return new Builder();
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

    public static final class Builder {
        private String queryId;
        private String subQueryId = "-";
        private Long lastUpdateTime;
        private Long expiryDate;
        private QueryState lastKnownState;
        private Long recordCount;

        private Builder() {
        }

        public Builder queryId(String queryId) {
            this.queryId = queryId;
            return this;
        }

        public Builder subQueryId(String subQueryId) {
            this.subQueryId = subQueryId;
            return this;
        }

        public Builder lastUpdateTime(Long lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
            return this;
        }

        public Builder expiryDate(Long expiryDate) {
            this.expiryDate = expiryDate;
            return this;
        }

        public Builder lastKnownState(QueryState lastKnownState) {
            this.lastKnownState = lastKnownState;
            return this;
        }

        public Builder recordCount(Long recordCount) {
            this.recordCount = recordCount;
            return this;
        }

        public TrackedQuery build() {
            return new TrackedQuery(this);
        }
    }
}
