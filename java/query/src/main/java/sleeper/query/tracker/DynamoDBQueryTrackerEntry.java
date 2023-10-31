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

import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DynamoDBQueryTrackerEntry {

    public static final String QUERY_ID = "queryId";
    public static final String LAST_UPDATE_TIME = "lastUpdateTime";
    public static final String LAST_KNOWN_STATE = "lastKnownState";
    public static final String RECORD_COUNT = "recordCount";
    public static final String SUB_QUERY_ID = "subQueryId";
    public static final String ERROR_MESSAGE = "errors";
    public static final String EXPIRY_DATE = "expiryDate";

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

    public static final class Builder {
        private String queryId;
        private String subQueryId;
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

        public DynamoDBQueryTrackerEntry build() {
            return new DynamoDBQueryTrackerEntry(this);
        }
    }
}
