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

package sleeper.query.runner.tracker;

import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;

import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.tracker.QueryState;
import sleeper.query.core.tracker.TrackedQuery;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static sleeper.query.runner.tracker.DynamoDBQueryTracker.NON_NESTED_QUERY_PLACEHOLDER;

/**
 * A model for entries in the query tracker DynamoDB table. Will be mapped to {@link TrackedQuery} objects.
 */
class DynamoDBQueryTrackerEntry {

    static final String QUERY_ID = "queryId";
    static final String LAST_UPDATE_TIME = "lastUpdateTime";
    static final String LAST_KNOWN_STATE = "lastKnownState";
    static final String RECORD_COUNT = "recordCount";
    static final String SUB_QUERY_ID = "subQueryId";
    static final String ERROR_MESSAGE = "errors";
    static final String EXPIRY_DATE = "expiryDate";

    private final String queryId;
    private final String subQueryId;
    private final QueryState state;
    private final long recordCount;
    private final String errorMessage;

    private DynamoDBQueryTrackerEntry(Builder builder) {
        queryId = builder.queryId;
        subQueryId = builder.subQueryId;
        state = builder.state;
        recordCount = builder.recordCount;
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
        key.put(QUERY_ID, new AttributeValue(queryId));
        key.put(SUB_QUERY_ID, new AttributeValue(subQueryId));
        return key;
    }

    public Map<String, AttributeValueUpdate> getValueUpdate(long queryTrackerTTL) {
        Map<String, AttributeValueUpdate> valueUpdate = new HashMap<>();
        long now = System.currentTimeMillis() / 1000;
        long expiryDate = now + (3600 * 24 * queryTrackerTTL);
        valueUpdate.put(LAST_UPDATE_TIME, new AttributeValueUpdate(
                new AttributeValue().withN(String.valueOf(now)), AttributeAction.PUT));

        valueUpdate.put(EXPIRY_DATE, new AttributeValueUpdate(
                new AttributeValue().withN(String.valueOf(expiryDate)), AttributeAction.PUT));

        valueUpdate.put(RECORD_COUNT, new AttributeValueUpdate(
                new AttributeValue().withN(String.valueOf(recordCount)), AttributeAction.PUT));

        valueUpdate.put(LAST_KNOWN_STATE, new AttributeValueUpdate(
                new AttributeValue(state.name()), AttributeAction.PUT));
        if (Objects.nonNull(errorMessage)) {
            valueUpdate.put(ERROR_MESSAGE, new AttributeValueUpdate(
                    new AttributeValue().withS(errorMessage), AttributeAction.PUT));
        }
        return valueUpdate;
    }

    public static TrackedQuery toTrackedQuery(Map<String, AttributeValue> stringAttributeValueMap) {
        String id = stringAttributeValueMap.get(QUERY_ID).getS();
        Long updateTime = Long.valueOf(stringAttributeValueMap.get(LAST_UPDATE_TIME).getN());
        Long expiryDate = Long.valueOf(stringAttributeValueMap.get(EXPIRY_DATE).getN());
        Long recordCount = Long.valueOf(stringAttributeValueMap.get(RECORD_COUNT).getN());
        QueryState state = QueryState.valueOf(stringAttributeValueMap.get(LAST_KNOWN_STATE).getS());
        String subQueryId = stringAttributeValueMap.get(SUB_QUERY_ID).getS();
        String errorMessage = null;
        if (stringAttributeValueMap.containsKey(ERROR_MESSAGE)) {
            errorMessage = stringAttributeValueMap.get(ERROR_MESSAGE).getS();
        }

        return TrackedQuery.builder()
                .queryId(id).subQueryId(subQueryId)
                .lastUpdateTime(updateTime)
                .expiryDate(expiryDate)
                .lastKnownState(state)
                .recordCount(recordCount)
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
                .recordCount(totalRecordCount)
                .errorMessage(errorMessage)
                .build();
    }

    static final class Builder {
        private String queryId;
        private String subQueryId = NON_NESTED_QUERY_PLACEHOLDER;
        private QueryState state;
        private long recordCount;
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

        public Builder recordCount(long recordCount) {
            this.recordCount = recordCount;
            return this;
        }

        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public Builder completed(ResultsOutputInfo outputInfo) {
            if (outputInfo.getError() != null) {
                if (outputInfo.getRecordCount() > 0) {
                    return state(QueryState.PARTIALLY_FAILED)
                            .recordCount(outputInfo.getRecordCount())
                            .errorMessage(outputInfo.getError().getMessage());
                } else {
                    return state(QueryState.FAILED)
                            .errorMessage(outputInfo.getError().getMessage());
                }
            } else {
                return state(QueryState.COMPLETED)
                        .recordCount(outputInfo.getRecordCount());
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
