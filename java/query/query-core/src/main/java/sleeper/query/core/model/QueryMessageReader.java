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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Reads queries from messages on a query queue, loading the query from a store if the message points to one. This is
 * the counterpart to {@link QueryMessageUploader}.
 * <p>
 * A message holds either a query serialised to JSON, or a pointer to where a large query has been stored. Both are read
 * with the same model, so a message is only parsed once. A pointer message contains the key the query is stored under,
 * and may optionally hold the query ID. The query ID is used to report a failure if the stored query cannot be read,
 * and must match the ID of the stored query. No other fields are allowed in a pointer message.
 */
public class QueryMessageReader {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryMessageReader.class);

    private final QueryJsonStore jsonStore;
    private final QuerySerDe querySerDe;

    public QueryMessageReader(QueryJsonStore jsonStore, QuerySerDe querySerDe) {
        this.jsonStore = jsonStore;
        this.querySerDe = querySerDe;
    }

    /**
     * Reads a query from a message, loading it from the store if the message points to a stored query.
     *
     * @param  message the message body
     * @return         the query
     */
    public QueryOrLeafPartitionQuery readAndLoadIfStored(String message) {
        QueryJson json = querySerDe.parseQueryJson(message);
        String key = json.getQueryJsonS3Key();
        if (key == null) {
            return querySerDe.fromQueryJson(json);
        }
        String messageQueryId = json.getQueryId();
        if (!json.isOnlyPointerToStoredQuery()) {
            throw new QueryValidationException(messageQueryId, null,
                    "Message pointing to a stored query must only contain the fields queryId and " + QuerySerDe.STORED_QUERY_KEY_FIELD);
        }
        QueryOrLeafPartitionQuery query = loadStoredQuery(messageQueryId, key);
        if (messageQueryId != null && !Objects.equals(messageQueryId, query.getQueryId())) {
            throw new QueryValidationException(messageQueryId, null,
                    "Query ID in message does not match stored query at " + key + ", found \"" + query.getQueryId() + "\"");
        }
        return query;
    }

    private QueryOrLeafPartitionQuery loadStoredQuery(String messageQueryId, String key) {
        LOGGER.info("Loading query JSON from {}", key);
        String storedJson;
        try {
            storedJson = jsonStore.load(key);
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
