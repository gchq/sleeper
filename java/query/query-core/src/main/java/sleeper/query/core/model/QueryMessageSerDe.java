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
package sleeper.query.core.model;

import java.util.Objects;
import java.util.function.Function;

/**
 * Serialises messages for a query queue to and from JSON. A message holds either a query serialised to JSON, or a
 * pointer to where a large query has been stored. Both are read with the same model, so a message is only parsed once.
 * <p>
 * A pointer message holds the key the query is stored under in a field {@value #STORED_QUERY_KEY_FIELD}, and may
 * optionally hold the query ID. The query ID is used to report a failure if the stored query cannot be read, and must
 * match the ID of the stored query. No other fields are allowed in a pointer message.
 */
public class QueryMessageSerDe {

    public static final String STORED_QUERY_KEY_FIELD = "queryJsonS3Key";

    private final QuerySerDe querySerDe;

    public QueryMessageSerDe(QuerySerDe querySerDe) {
        this.querySerDe = querySerDe;
    }

    /**
     * Serialises a query to a message holding the query.
     *
     * @param  query the query
     * @return       the message body
     */
    public String toJson(Query query) {
        return querySerDe.toJson(query);
    }

    /**
     * Serialises a leaf partition query to a message holding the query.
     *
     * @param  query the leaf partition query
     * @return       the message body
     */
    public String toJson(LeafPartitionQuery query) {
        return querySerDe.toJson(query);
    }

    /**
     * Creates a message pointing to a query that has been stored separately.
     *
     * @param  queryId the query ID, used to report a failure if the stored query cannot be read
     * @param  key     the key the query JSON is stored under
     * @return         the message body
     */
    public String toJsonPointingToStoredQuery(String queryId, String key) {
        return querySerDe.toJsonPointingToStoredQuery(queryId, key);
    }

    /**
     * Deserialises a message. If the message points to a stored query, the query is loaded and deserialised.
     *
     * @param  message   the message body
     * @param  loadQuery a function to load the JSON of a stored query, given its key
     * @return           the query
     */
    public QueryOrLeafPartitionQuery fromJson(String message, Function<String, String> loadQuery) {
        QueryJson json = querySerDe.parseQueryJson(message);
        String key = json.getQueryJsonS3Key();
        if (key == null) {
            return querySerDe.fromQueryJson(json);
        }
        String messageQueryId = json.getQueryId();
        if (!json.isOnlyPointerToStoredQuery()) {
            throw new QueryValidationException(messageQueryId, null,
                    "Message pointing to a stored query must only contain the fields queryId and " + STORED_QUERY_KEY_FIELD);
        }
        QueryOrLeafPartitionQuery query = loadStoredQuery(messageQueryId, key, loadQuery);
        if (messageQueryId != null && !Objects.equals(messageQueryId, query.getQueryId())) {
            throw new QueryValidationException(messageQueryId, null,
                    "Query ID in message does not match stored query at " + key + ", found \"" + query.getQueryId() + "\"");
        }
        return query;
    }

    private QueryOrLeafPartitionQuery loadStoredQuery(String messageQueryId, String key, Function<String, String> loadQuery) {
        String storedJson;
        try {
            storedJson = loadQuery.apply(key);
        } catch (RuntimeException e) {
            if (messageQueryId == null) {
                throw e;
            }
            throw new QueryValidationException(messageQueryId, null, e);
        }
        QueryJson stored = querySerDe.parseQueryJson(storedJson);
        if (stored.getQueryJsonS3Key() != null) {
            throw new QueryValidationException(messageQueryId, null, "Stored query at " + key + " points to another stored query");
        }
        return querySerDe.fromQueryJson(stored);
    }
}
