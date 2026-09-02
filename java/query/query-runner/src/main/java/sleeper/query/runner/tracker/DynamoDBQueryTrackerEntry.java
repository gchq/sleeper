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

package sleeper.query.runner.tracker;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.output.ResultsOutputLocation;
import sleeper.query.core.tracker.QueryState;
import sleeper.query.core.tracker.TrackedQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A model for entries in the query tracker DynamoDB table. Will be mapped to {@link TrackedQuery} objects.
 */
class DynamoDBQueryTrackerEntry {

    static final String QUERY_ID = "queryId";
    static final String FIRST_UPDATE_TIME = "firstUpdateTime";
    static final String LAST_UPDATE_TIME = "lastUpdateTime";
    static final String LAST_KNOWN_STATE = "lastKnownState";
    static final String ROW_COUNT = "rowCount";
    static final String SUB_QUERY_ID = "subQueryId";
    static final String TABLE_ID = "tableId";
    static final String ERROR_MESSAGE = "errors";
    static final String EXPIRY_DATE = "expiryDate";
    static final String RESULTS_LOCATIONS = "resultsLocations";
    static final String LOCATION_TYPE = "type";
    static final String LOCATION_VALUE = "location";
    static final String NON_NESTED_QUERY_PLACEHOLDER = "-";

    private final String queryId;
    private final String subQueryId;
    private final String tableId;
    private final QueryState state;
    private final long rowCount;
    private final String errorMessage;
    private final List<ResultsOutputLocation> resultsLocations;

    private DynamoDBQueryTrackerEntry(Builder builder) {
        queryId = builder.queryId;
        subQueryId = builder.subQueryId;
        tableId = builder.tableId;
        state = builder.state;
        rowCount = builder.rowCount;
        errorMessage = builder.errorMessage;
        resultsLocations = builder.resultsLocations;
    }

    public static Builder withQuery(Query query) {
        return builder()
                .queryId(query.getQueryId())
                .tableId(query.getTableId());
    }

    public static Builder withLeafQuery(LeafPartitionQuery query) {
        return builder()
                .queryId(query.getQueryId())
                .subQueryId(query.getSubQueryId())
                .tableId(query.getTableId());
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

    public QueryState getState() {
        return state;
    }

    public long getRowCount() {
        return rowCount;
    }

    public String getTableId() {
        return tableId;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public List<ResultsOutputLocation> getResultsLocations() {
        return resultsLocations;
    }

    /**
     * Encodes the results locations as a DynamoDB list of maps, each holding the location's type and value.
     *
     * @return the attribute value, or null if there are no locations to store
     */
    public AttributeValue getResultsLocationsAttribute() {
        if (resultsLocations == null || resultsLocations.isEmpty()) {
            return null;
        }
        List<AttributeValue> encoded = resultsLocations.stream()
                .map(location -> AttributeValue.fromM(Map.of(
                        LOCATION_TYPE, AttributeValue.fromS(location.getType()),
                        LOCATION_VALUE, AttributeValue.fromS(location.getLocation()))))
                .toList();
        return AttributeValue.fromL(encoded);
    }

    private static List<ResultsOutputLocation> readResultsLocations(Map<String, AttributeValue> item) {
        AttributeValue attribute = item.get(RESULTS_LOCATIONS);
        if (attribute == null || !attribute.hasL()) {
            return new ArrayList<>();
        }
        List<ResultsOutputLocation> locations = new ArrayList<>();
        for (AttributeValue element : attribute.l()) {
            Map<String, AttributeValue> map = element.m();
            AttributeValue type = map.get(LOCATION_TYPE);
            AttributeValue value = map.get(LOCATION_VALUE);
            if (type != null && value != null) {
                locations.add(new ResultsOutputLocation(type.s(), value.s()));
            }
        }
        return locations;
    }

    public static TrackedQuery toTrackedQuery(Map<String, AttributeValue> stringAttributeValueMap) {
        String id = stringAttributeValueMap.get(QUERY_ID).s();
        Long firstUpdateTime = Long.valueOf(stringAttributeValueMap.get(FIRST_UPDATE_TIME).n());
        Long updateTime = Long.valueOf(stringAttributeValueMap.get(LAST_UPDATE_TIME).n());
        Long expiryDate = Long.valueOf(stringAttributeValueMap.get(EXPIRY_DATE).n());
        Long rowCount = Long.valueOf(stringAttributeValueMap.get(ROW_COUNT).n());
        QueryState state = QueryState.valueOf(stringAttributeValueMap.get(LAST_KNOWN_STATE).s());
        String subQueryId = stringAttributeValueMap.get(SUB_QUERY_ID).s();
        String tableId = null;
        if (stringAttributeValueMap.containsKey(TABLE_ID)) {
            tableId = stringAttributeValueMap.get(TABLE_ID).s();
        }
        String errorMessage = null;
        if (stringAttributeValueMap.containsKey(ERROR_MESSAGE)) {
            errorMessage = stringAttributeValueMap.get(ERROR_MESSAGE).s();
        }

        return TrackedQuery.builder()
                .queryId(id).subQueryId(subQueryId)
                .tableId(tableId)
                .firstUpdateTime(firstUpdateTime)
                .lastUpdateTime(updateTime)
                .expiryDate(expiryDate)
                .lastKnownState(state)
                .rowCount(rowCount)
                .errorMessage(errorMessage)
                .resultsLocations(readResultsLocations(stringAttributeValueMap))
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

    public DynamoDBQueryTrackerEntry updateParent(QueryState state, long totalRowCount) {
        return builder()
                .queryId(queryId)
                .tableId(tableId)
                .state(state)
                .rowCount(totalRowCount)
                .errorMessage(errorMessage)
                .build();
    }

    static final class Builder {
        private String queryId;
        private String subQueryId = NON_NESTED_QUERY_PLACEHOLDER;
        private String tableId;
        private QueryState state;
        private long rowCount;
        private String errorMessage;
        private List<ResultsOutputLocation> resultsLocations = new ArrayList<>();

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

        public Builder tableId(String tableId) {
            this.tableId = tableId;
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

        public Builder resultsLocations(List<ResultsOutputLocation> resultsLocations) {
            this.resultsLocations = resultsLocations == null ? new ArrayList<>() : new ArrayList<>(resultsLocations);
            return this;
        }

        public Builder completed(ResultsOutputInfo outputInfo) {
            resultsLocations(outputInfo.getLocations());
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
