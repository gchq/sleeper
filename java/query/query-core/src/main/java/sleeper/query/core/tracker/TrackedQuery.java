/*
 * Copyright 2022-2026 Crown Copyright
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

import sleeper.query.core.output.ResultsOutputInfo;

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
    private final Long expectedSubQueryCount;
    private final Long succeededSubQueryCount;
    private final Long failedSubQueryCount;
    private final Long finishedSubQueryRowCount;

    private TrackedQuery(Builder builder) {
        queryId = builder.queryId;
        subQueryId = builder.subQueryId;
        lastUpdateTime = builder.lastUpdateTime;
        expiryDate = builder.expiryDate;
        lastKnownState = builder.lastKnownState;
        rowCount = builder.rowCount;
        errorMessage = builder.errorMessage;
        expectedSubQueryCount = builder.expectedSubQueryCount;
        succeededSubQueryCount = builder.succeededSubQueryCount;
        failedSubQueryCount = builder.failedSubQueryCount;
        finishedSubQueryRowCount = builder.finishedSubQueryRowCount;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return builder().queryId(queryId).subQueryId(subQueryId)
                .lastUpdateTime(lastUpdateTime).expiryDate(expiryDate)
                .lastKnownState(lastKnownState).rowCount(rowCount).errorMessage(errorMessage)
                .expectedSubQueryCount(expectedSubQueryCount)
                .succeededSubQueryCount(succeededSubQueryCount)
                .failedSubQueryCount(failedSubQueryCount)
                .finishedSubQueryRowCount(finishedSubQueryRowCount);
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

    public Long getExpectedSubQueryCount() {
        return expectedSubQueryCount;
    }

    public Long getSucceededSubQueryCount() {
        return succeededSubQueryCount;
    }

    public Long getFailedSubQueryCount() {
        return failedSubQueryCount;
    }

    public Long getFinishedSubQueryRowCount() {
        return finishedSubQueryRowCount;
    }

    /**
     * Retrieves the number of sub-queries that have finished, whether they succeeded or failed. This is only known
     * for a parent query that was split into sub-queries.
     *
     * @return the number of sub-queries that have finished, or null if that is not known
     */
    public Long getFinishedSubQueryCount() {
        if (expectedSubQueryCount == null) {
            return null;
        }
        return orZero(succeededSubQueryCount) + orZero(failedSubQueryCount);
    }

    /**
     * Retrieves the number of sub-queries that have not finished yet. This is only known for a parent query that was
     * split into sub-queries.
     *
     * @return the number of sub-queries still to finish, or null if that is not known
     */
    public Long getRemainingSubQueryCount() {
        if (expectedSubQueryCount == null) {
            return null;
        }
        return Math.max(0, expectedSubQueryCount - getFinishedSubQueryCount());
    }

    private static long orZero(Long count) {
        return count != null ? count : 0;
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
                && Objects.equals(errorMessage, that.errorMessage)
                && Objects.equals(expectedSubQueryCount, that.expectedSubQueryCount)
                && Objects.equals(succeededSubQueryCount, that.succeededSubQueryCount)
                && Objects.equals(failedSubQueryCount, that.failedSubQueryCount)
                && Objects.equals(finishedSubQueryRowCount, that.finishedSubQueryRowCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryId, subQueryId, lastUpdateTime, expiryDate, lastKnownState, rowCount, errorMessage,
                expectedSubQueryCount, succeededSubQueryCount, failedSubQueryCount, finishedSubQueryRowCount);
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
                ", expectedSubQueryCount=" + expectedSubQueryCount +
                ", succeededSubQueryCount=" + succeededSubQueryCount +
                ", failedSubQueryCount=" + failedSubQueryCount +
                ", finishedSubQueryRowCount=" + finishedSubQueryRowCount +
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
        private Long expectedSubQueryCount;
        private Long succeededSubQueryCount;
        private Long failedSubQueryCount;
        private Long finishedSubQueryRowCount;

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

        /**
         * Provides the number of sub-queries the query was split into. This is only set on a parent query that was
         * split into sub-queries.
         *
         * @param  expectedSubQueryCount the number of sub-queries, or null if that is not known
         * @return                       the builder
         */
        public Builder expectedSubQueryCount(Long expectedSubQueryCount) {
            this.expectedSubQueryCount = expectedSubQueryCount;
            return this;
        }

        /**
         * Provides the number of sub-queries that have finished successfully. This is only set on a parent query
         * that was split into sub-queries.
         *
         * @param  succeededSubQueryCount the number of successful sub-queries, or null if that is not known
         * @return                        the builder
         */
        public Builder succeededSubQueryCount(Long succeededSubQueryCount) {
            this.succeededSubQueryCount = succeededSubQueryCount;
            return this;
        }

        /**
         * Provides the number of sub-queries that have failed, fully or partially. This is only set on a parent
         * query that was split into sub-queries.
         *
         * @param  failedSubQueryCount the number of failed sub-queries, or null if that is not known
         * @return                     the builder
         */
        public Builder failedSubQueryCount(Long failedSubQueryCount) {
            this.failedSubQueryCount = failedSubQueryCount;
            return this;
        }

        /**
         * Provides the total number of rows output so far by sub-queries that have finished. This is only set on a
         * parent query that was split into sub-queries.
         *
         * @param  finishedSubQueryRowCount the number of rows output by finished sub-queries, or null if that is not
         *                                  known
         * @return                          the builder
         */
        public Builder finishedSubQueryRowCount(Long finishedSubQueryRowCount) {
            this.finishedSubQueryRowCount = finishedSubQueryRowCount;
            return this;
        }

        /**
         * Provides information on the results of the query.
         *
         * @param  outputInfo the information
         * @return            the builder
         */
        public Builder outputInfo(ResultsOutputInfo outputInfo) {
            return rowCount(outputInfo.getRowCount())
                    .error(outputInfo.getError());
        }

        /**
         * Provides the error.
         *
         * @param  error the error
         * @return       the builder
         */
        public Builder error(Exception error) {
            return errorMessage(error == null ? null : error.getMessage());
        }

        public TrackedQuery build() {
            return new TrackedQuery(this);
        }
    }
}
