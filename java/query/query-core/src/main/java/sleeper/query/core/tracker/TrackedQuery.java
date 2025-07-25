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

import java.time.Instant;
import java.util.Objects;

/**
 * Contains information about a query including its ID and current status.
 * <p>
 * This class encapsulates key details required for tracking the lifecycle and outcome of a query.
 * It provides storage for query-related metadata, such as its unique identifier,
 * sub-query details, timestamps for updates and expiry, its last known state,
 * row count, and any associated error messages.
 *
 */
public class TrackedQuery {
    private final String queryId;
    private final String subQueryId;
    private final Long lastUpdateTime;
    private final Long expiryDate;
    private final QueryState lastKnownState;
    private final Long rowCount;
    private final String errorMessage;

    private TrackedQuery(Builder builder) {
        queryId = builder.queryId;
        subQueryId = builder.subQueryId;
        lastUpdateTime = builder.lastUpdateTime;
        expiryDate = builder.expiryDate;
        lastKnownState = builder.lastKnownState;
        rowCount = builder.rowCount;
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

    public Long getRowCount() {
        return rowCount;
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
                && Objects.equals(rowCount, that.rowCount)
                && Objects.equals(errorMessage, that.errorMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryId, subQueryId, lastUpdateTime, expiryDate, lastKnownState, rowCount, errorMessage);
    }

    @Override
    public String toString() {
        return "TrackedQuery{" +
                "queryId='" + queryId + '\'' +
                ", subQueryId='" + subQueryId + '\'' +
                ", lastUpdateTime=" + lastUpdateTime +
                ", expiryDate=" + expiryDate +
                ", lastKnownState=" + lastKnownState +
                ", rowCount=" + rowCount +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
    }

    /**
     * Builder for this class.
     */
    public static final class Builder {
        private String queryId;
        private String subQueryId = "-";
        private Long lastUpdateTime;
        private Long expiryDate;
        private QueryState lastKnownState;
        private Long rowCount = 0L;
        private String errorMessage;

        private Builder() {
        }

        /**
         * Provides the query ID.
         *
         * @param  queryId the query ID
         * @return         the builder
         */
        public Builder queryId(String queryId) {
            this.queryId = queryId;
            return this;
        }

        /**
         * Provides the sub query ID.
         *
         * @param  subQueryId the sub query ID
         * @return            the builder
         */
        public Builder subQueryId(String subQueryId) {
            this.subQueryId = subQueryId;
            return this;
        }

        /**
         * Provides the last update time.
         *
         * @param  lastUpdateTime the last update time
         * @return                the builder
         */
        public Builder lastUpdateTime(Instant lastUpdateTime) {
            return lastUpdateTime(lastUpdateTime.toEpochMilli());
        }

        /**
         * Provides the last update time.
         *
         * @param  lastUpdateTime the last update time in milliseconds since the epoch
         * @return                the builder
         */
        public Builder lastUpdateTime(Long lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
            return this;
        }

        /**
         * Provides the expiry date.
         *
         * @param  expiryDate the expiry date
         * @return            the builder
         */
        public Builder expiryDate(Instant expiryDate) {
            return expiryDate(expiryDate.toEpochMilli());
        }

        /**
         * Provides the expiry date.
         *
         * @param  expiryDate the expiry date in milliseconds since the epoch
         * @return            the builder
         */
        public Builder expiryDate(Long expiryDate) {
            this.expiryDate = expiryDate;
            return this;
        }

        /**
         * Provides the last known query state.
         *
         * @param  lastKnownState the last known state
         * @return                the builder
         */
        public Builder lastKnownState(QueryState lastKnownState) {
            this.lastKnownState = lastKnownState;
            return this;
        }

        /**
         * Provides the number of rows returned by the query.
         *
         * @param  rowCount the number of rows returned
         * @return          the builder
         */
        public Builder rowCount(Long rowCount) {
            this.rowCount = rowCount;
            return this;
        }

        /**
         * Provides the error message.
         *
         * @param  errorMessage the error message
         * @return              the builder
         */
        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public TrackedQuery build() {
            return new TrackedQuery(this);
        }
    }
}
