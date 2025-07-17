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
package sleeper.query.runner.websocket;

import sleeper.query.core.output.ResultsOutputLocation;

import java.util.List;
import java.util.Objects;

public class QueryWebSocketStatusMessage {

    private final QueryWebSocketMessageType message;
    private final String queryId;
    private final List<String> queryIds;
    private final Long rowCount;
    private final List<ResultsOutputLocation> locations;
    private final String error;

    private QueryWebSocketStatusMessage(Builder builder) {
        message = builder.message;
        queryId = builder.queryId;
        queryIds = builder.queryIds;
        rowCount = builder.rowCount;
        locations = builder.locations;
        error = builder.error;
    }

    public static QueryWebSocketStatusMessage queryWasSplitToSubqueries(String queryId, List<String> subQueryIds) {
        return builder()
                .message(QueryWebSocketMessageType.subqueries)
                .queryId(queryId)
                .queryIds(subQueryIds)
                .build();
    }

    public static QueryWebSocketStatusMessage queryCompleted(String queryId, long rowCount, List<ResultsOutputLocation> locations) {
        return builder()
                .message(QueryWebSocketMessageType.completed)
                .queryId(queryId)
                .rowCount(rowCount)
                .locations(locations)
                .build();
    }

    public static QueryWebSocketStatusMessage queryError(String queryId, String error, long rowCount, List<ResultsOutputLocation> locations) {
        return builder()
                .message(QueryWebSocketMessageType.completed)
                .queryId(queryId)
                .error(error)
                .rowCount(rowCount)
                .locations(locations)
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public QueryWebSocketMessageType getMessage() {
        return message;
    }

    public String getQueryId() {
        return queryId;
    }

    public List<String> getQueryIds() {
        return queryIds;
    }

    public long getRowCount() {
        return rowCount;
    }

    public List<ResultsOutputLocation> getLocations() {
        return locations;
    }

    public String getError() {
        return error;
    }

    @Override
    public String toString() {
        return "QueryWebSocketMessage{message=" + message + ", queryId=" + queryId + ", queryIds=" + queryIds + ", rowCount=" + rowCount + ", locations=" + locations + ", error="
                + error + "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, queryId, queryIds, rowCount, locations, error);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof QueryWebSocketStatusMessage)) {
            return false;
        }
        QueryWebSocketStatusMessage other = (QueryWebSocketStatusMessage) obj;
        return message == other.message && Objects.equals(queryId, other.queryId) && Objects.equals(queryIds, other.queryIds) && rowCount == other.rowCount
                && Objects.equals(locations, other.locations) && Objects.equals(error, other.error);
    }

    public static class Builder {
        private QueryWebSocketMessageType message;
        private String queryId;
        private List<String> queryIds;
        private Long rowCount;
        private List<ResultsOutputLocation> locations;
        private String error;

        private Builder() {
        }

        public Builder message(QueryWebSocketMessageType message) {
            this.message = message;
            return this;
        }

        public Builder queryId(String queryId) {
            this.queryId = queryId;
            return this;
        }

        public Builder queryIds(List<String> queryIds) {
            this.queryIds = queryIds;
            return this;
        }

        public Builder rowCount(long rowCount) {
            this.rowCount = rowCount;
            return this;
        }

        public Builder locations(List<ResultsOutputLocation> locations) {
            this.locations = locations;
            return this;
        }

        public Builder error(String error) {
            this.error = error;
            return this;
        }

        public QueryWebSocketStatusMessage build() {
            return new QueryWebSocketStatusMessage(this);
        }
    }

}
