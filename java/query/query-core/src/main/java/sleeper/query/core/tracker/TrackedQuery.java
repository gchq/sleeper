/*
 * Copyright 2022-2024 Crown Copyright
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

import java.time.Instant;
import java.util.Objects;

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
    private final String errorMessage;

    private TrackedQuery(Builder builder) {
        queryId = builder.queryId;
        subQueryId = builder.subQueryId;
        lastUpdateTime = builder.lastUpdateTime;
        expiryDate = builder.expiryDate;
        lastKnownState = builder.lastKnownState;
        recordCount = builder.recordCount;
        errorMessage = builder.errorMessage;
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

    public String getErrorMessage() {
        return errorMessage;
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
        return Objects.equals(queryId, that.queryId)
                && Objects.equals(subQueryId, that.subQueryId)
                && Objects.equals(lastUpdateTime, that.lastUpdateTime)
                && Objects.equals(expiryDate, that.expiryDate)
                && lastKnownState == that.lastKnownState
                && Objects.equals(recordCount, that.recordCount)
                && Objects.equals(errorMessage, that.errorMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryId, subQueryId, lastUpdateTime, expiryDate, lastKnownState, recordCount, errorMessage);
    }

    @Override
    public String toString() {
        return "TrackedQuery{" +
                "queryId='" + queryId + '\'' +
                ", subQueryId='" + subQueryId + '\'' +
                ", lastUpdateTime=" + lastUpdateTime +
                ", expiryDate=" + expiryDate +
                ", lastKnownState=" + lastKnownState +
                ", recordCount=" + recordCount +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
    }

    public static final class Builder {
        private String queryId;
        private String subQueryId = "-";
        private Long lastUpdateTime;
        private Long expiryDate;
        private QueryState lastKnownState;
        private Long recordCount = 0L;
        private String errorMessage;

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

        public Builder lastUpdateTime(Instant lastUpdateTime) {
            return lastUpdateTime(lastUpdateTime.toEpochMilli());
        }

        public Builder lastUpdateTime(Long lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
            return this;
        }

        public Builder expiryDate(Instant expiryDate) {
            return expiryDate(expiryDate.toEpochMilli());
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

        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public TrackedQuery build() {
            return new TrackedQuery(this);
        }
    }
}
