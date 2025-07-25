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

package sleeper.query.runner.tracker;

import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;

import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.tracker.QueryState;
import sleeper.query.core.tracker.TrackedQuery;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A model for entries in the query tracker DynamoDB table. Will be mapped to {@link TrackedQuery} objects.
 */
class DynamoDBQueryTrackerEntry {

    static final String QUERY_ID = "queryId";
    static final String LAST_UPDATE_TIME = "lastUpdateTime";
    static final String LAST_KNOWN_STATE = "lastKnownState";
    static final String ROW_COUNT = "rowCount";
    static final String SUB_QUERY_ID = "subQueryId";
    static final String ERROR_MESSAGE = "errors";
    static final String EXPIRY_DATE = "expiryDate";
    static final String NON_NESTED_QUERY_PLACEHOLDER = "-";

    private final String queryId;
    private final String subQueryId;
    private final QueryState state;
    private final long rowCount;
    private final String errorMessage;

    private DynamoDBQueryTrackerEntry(Builder builder) {
        queryId = builder.queryId;
        subQueryId = builder.subQueryId;
        state = builder.state;
        rowCount = builder.rowCount;
        errorMessage = builder.errorMessage;
    }

    public static Builder withQuery(Query query) {
        return builder().queryId(query.getQueryId());
    }

    public static Builder withLeafQuery(LeafPartitionQuery query) {
        return builder()
                .queryId(query.getQueryId())
                .subQueryId(query.getSubQueryId());
    }

    public static Builder builder() {
        return new Builder();
    }

    public Map<String, AttributeValue> getKey() {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(QUERY_ID, AttributeValue.fromS(queryId));
        key.put(SUB_QUERY_ID, AttributeValue.fromS(subQueryId));
        return key;
    }

    public Map<String, AttributeValueUpdate> getValueUpdate(long queryTrackerTTL) {
        Map<String, AttributeValueUpdate> valueUpdate = new HashMap<>();
        long now = System.currentTimeMillis() / 1000;
        long expiryDate = now + (3600 * 24 * queryTrackerTTL);
        valueUpdate.put(LAST_UPDATE_TIME, AttributeValueUpdate.builder()
                .value(AttributeValue.fromN(String.valueOf(now)))
                .action(AttributeAction.PUT)
                .build());
        valueUpdate.put(EXPIRY_DATE, AttributeValueUpdate.builder()
                .value(AttributeValue.fromN(String.valueOf(expiryDate)))
                .action(AttributeAction.PUT)
                .build());
        valueUpdate.put(ROW_COUNT, AttributeValueUpdate.builder()
                .value(AttributeValue.fromN(String.valueOf(rowCount)))
                .action(AttributeAction.PUT)
                .build());
        valueUpdate.put(LAST_KNOWN_STATE, AttributeValueUpdate.builder()
                .value(AttributeValue.fromS(state.name()))
                .action(AttributeAction.PUT)
                .build());
        if (Objects.nonNull(errorMessage)) {
            valueUpdate.put(ERROR_MESSAGE, AttributeValueUpdate.builder()
                    .value(AttributeValue.fromS(errorMessage))
                    .action(AttributeAction.PUT)
                    .build());
        }
        return valueUpdate;
    }

    public static TrackedQuery toTrackedQuery(Map<String, AttributeValue> stringAttributeValueMap) {
        String id = stringAttributeValueMap.get(QUERY_ID).s();
        Long updateTime = Long.valueOf(stringAttributeValueMap.get(LAST_UPDATE_TIME).n());
        Long expiryDate = Long.valueOf(stringAttributeValueMap.get(EXPIRY_DATE).n());
        Long rowCount = Long.valueOf(stringAttributeValueMap.get(ROW_COUNT).n());
        QueryState state = QueryState.valueOf(stringAttributeValueMap.get(LAST_KNOWN_STATE).s());
        String subQueryId = stringAttributeValueMap.get(SUB_QUERY_ID).s();
        String errorMessage = null;
        if (stringAttributeValueMap.containsKey(ERROR_MESSAGE)) {
            errorMessage = stringAttributeValueMap.get(ERROR_MESSAGE).s();
        }

        return TrackedQuery.builder()
                .queryId(id).subQueryId(subQueryId)
                .lastUpdateTime(updateTime)
                .expiryDate(expiryDate)
                .lastKnownState(state)
                .rowCount(rowCount)
                .errorMessage(errorMessage)
                .build();
    }

    public boolean isUpdateParent() {
        return isSubQuery() &&
                (state.equals(QueryState.COMPLETED) || state.equals(QueryState.FAILED));
    }

    private boolean isSubQuery() {
        return !NON_NESTED_QUERY_PLACEHOLDER.equals(subQueryId);
    }

    public String getQueryId() {
        return queryId;
    }

    public DynamoDBQueryTrackerEntry updateParent(QueryState state, long totalRecordCount) {
        return builder()
                .queryId(queryId)
                .state(state)
                .rowCount(totalRecordCount)
                .errorMessage(errorMessage)
                .build();
    }

    static final class Builder {
        private String queryId;
        private String subQueryId = NON_NESTED_QUERY_PLACEHOLDER;
        private QueryState state;
        private long rowCount;
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

        public Builder state(QueryState state) {
            this.state = state;
            return this;
        }

        public Builder rowCount(long rowCount) {
            this.rowCount = rowCount;
            return this;
        }

        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public Builder completed(ResultsOutputInfo outputInfo) {
            if (outputInfo.getError() != null) {
                if (outputInfo.getRowCount() > 0) {
                    return state(QueryState.PARTIALLY_FAILED)
                            .rowCount(outputInfo.getRowCount())
                            .errorMessage(outputInfo.getError().getMessage());
                } else {
                    return state(QueryState.FAILED)
                            .errorMessage(outputInfo.getError().getMessage());
                }
            } else {
                return state(QueryState.COMPLETED)
                        .rowCount(outputInfo.getRowCount());
            }
        }

        public Builder failed(Exception e) {
            return state(QueryState.FAILED)
                    .errorMessage(e.getMessage());
        }

        public DynamoDBQueryTrackerEntry build() {
            return new DynamoDBQueryTrackerEntry(this);
        }
    }
}
