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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Serialises queries to and from messages to be sent to a queue. If a query is too large to fit in a message, the JSON
 * is held in a {@link QueryJsonStore}, and the message only holds the key where it is stored.
 */
public class QueryMessageSerDe {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryMessageSerDe.class);

    /**
     * Minimum length of a JSON string that will be held in the store rather than the message.
     * <p>
     * Max size of an SQS message is 256KiB. Setting this to less than 256KiB leaves room for message attributes.
     * https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html
     */
    public static final int DEFAULT_JSON_LENGTH_TO_STORE = 1024 * 200;
    public static final String STORED_JSON_KEY_FIELD = "queryJsonS3Key";

    private static final Gson GSON = new Gson();

    private final QuerySerDe querySerDe;
    private final QueryJsonStore jsonStore;
    private final int jsonLengthToStore;
    private final Supplier<String> uuidSupplier;

    public QueryMessageSerDe(QuerySerDe querySerDe, QueryJsonStore jsonStore) {
        this(querySerDe, jsonStore, DEFAULT_JSON_LENGTH_TO_STORE, () -> UUID.randomUUID().toString());
    }

    public QueryMessageSerDe(QuerySerDe querySerDe, QueryJsonStore jsonStore, int jsonLengthToStore, Supplier<String> uuidSupplier) {
        this.querySerDe = querySerDe;
        this.jsonStore = jsonStore;
        this.jsonLengthToStore = jsonLengthToStore;
        this.uuidSupplier = uuidSupplier;
    }

    /**
     * Serialises a query to a message, storing the JSON separately if it is too big.
     *
     * @param  query the query
     * @return       the message body
     */
    public String toMessage(Query query) {
        return storeIfTooBig(querySerDe.toJson(query), query.getQueryId(), "query");
    }

    /**
     * Serialises a leaf partition query to a message, storing the JSON separately if it is too big.
     *
     * @param  query the leaf partition query
     * @return       the message body
     */
    public String toMessage(LeafPartitionQuery query) {
        return storeIfTooBig(querySerDe.toJson(query), query.getQueryId(), query.getSubQueryId());
    }

    /**
     * Deserialises a query from a message, loading the JSON from the store if it was held separately.
     *
     * @param  message the message body
     * @return         the query
     */
    public QueryOrLeafPartitionQuery fromMessage(String message) {
        return querySerDe.fromJsonOrLeafQuery(loadIfStored(message));
    }

    /**
     * Retrieves the query JSON for a message, loading it from the store if it was held separately.
     *
     * @param  message the message body
     * @return         the query JSON
     */
    public String loadIfStored(String message) {
        return readStoredKey(message)
                .map(key -> {
                    LOGGER.info("Loading query JSON from {}", key);
                    return jsonStore.load(key);
                })
                .orElse(message);
    }

    /**
     * Creates an object key to store the JSON of a query.
     *
     * @param  queryId    the query ID
     * @param  subQueryId the sub-query ID, or "query" for a parent query
     * @param  uuid       a unique identifier for this upload
     * @return            the object key
     */
    public static String createKey(String queryId, String subQueryId, String uuid) {
        return "queries/" + queryId + "/" + subQueryId + "-" + uuid + ".json";
    }

    private String storeIfTooBig(String json, String queryId, String subQueryId) {
        if (json.length() < jsonLengthToStore) {
            return json;
        }
        String key = createKey(queryId, subQueryId, uuidSupplier.get());
        LOGGER.info("Query JSON of length {} is too large for a message, storing at {}", json.length(), key);
        jsonStore.store(key, json);
        JsonObject pointer = new JsonObject();
        pointer.addProperty(STORED_JSON_KEY_FIELD, key);
        return GSON.toJson(pointer);
    }

    private static Optional<String> readStoredKey(String message) {
        // Avoid parsing large messages, as the pointer message is always small
        if (message.length() > 4096 || !message.contains(STORED_JSON_KEY_FIELD)) {
            return Optional.empty();
        }
        try {
            JsonElement element = JsonParser.parseString(message);
            if (!element.isJsonObject()) {
                return Optional.empty();
            }
            JsonObject object = element.getAsJsonObject();
            JsonElement key = object.get(STORED_JSON_KEY_FIELD);
            if (key == null || !key.isJsonPrimitive() || object.size() != 1) {
                return Optional.empty();
            }
            return Optional.of(key.getAsString());
        } catch (JsonSyntaxException e) {
            return Optional.empty();
        }
    }
}
