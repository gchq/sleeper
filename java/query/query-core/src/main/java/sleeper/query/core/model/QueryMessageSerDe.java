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

import java.util.Optional;

/**
 * Serialises messages for a query queue to and from JSON. The message may contain the query serialissed to JSON or
 * if it is a large query a pointer to where the query is stored.
 */
public class QueryMessageSerDe {

    public static final String STORED_QUERY_KEY_FIELD = "queryJsonS3Key";

    /**
     * Maximum length of a message that may be a pointer to a stored query. Longer messages are not checked for a
     * pointer, to avoid parsing large queries twice.
     */
    private static final int MAX_POINTER_MESSAGE_LENGTH = 4096;
    private static final Gson GSON = new Gson();

    private final QuerySerDe querySerDe;

    public QueryMessageSerDe(QuerySerDe querySerDe) {
        this.querySerDe = querySerDe;
    }

    /**
     * Serialises a query to a string.
     *
     * @param  query the query
     * @return       the message body
     */
    public String toJson(Query query) {
        return querySerDe.toJson(query);
    }

    /**
     * Serialises a leaf partition query to a string.
     *
     * @param  query the leaf partition query
     * @return       the message body
     */
    public String toJson(LeafPartitionQuery query) {
        return querySerDe.toJson(query);
    }

    /**
     * Creates a string containing a pointer to a query whose JSON has been stored in a store.
     *
     * @param  key the key the JSON is stored under
     * @return     the message body
     */
    public String toJsonPointingToStoredQuery(String key) {
        JsonObject pointer = new JsonObject();
        pointer.addProperty(STORED_QUERY_KEY_FIELD, key);
        return GSON.toJson(pointer);
    }

    /**
     * Reads the key of a stored query if the string points to one rather than holding the query directly.
     *
     * @param  message the message body
     * @return         the key the query JSON is stored under, if the message is a pointer, otherwise empty
     */
    public Optional<String> readStoredQueryKey(String message) {
        if (message.length() > MAX_POINTER_MESSAGE_LENGTH || !message.contains(STORED_QUERY_KEY_FIELD)) {
            return Optional.empty();
        }
        try {
            JsonElement element = JsonParser.parseString(message);
            if (!element.isJsonObject()) {
                return Optional.empty();
            }
            JsonObject object = element.getAsJsonObject();
            JsonElement key = object.get(STORED_QUERY_KEY_FIELD);
            if (key == null || !key.isJsonPrimitive() || object.size() != 1) {
                return Optional.empty();
            }
            return Optional.of(key.getAsString());
        } catch (JsonSyntaxException e) {
            return Optional.empty();
        }
    }

    /**
     * Deserialises the JSON of a query or leaf partition query.
     *
     * @param  json the query JSON
     * @return      the query
     */
    public QueryOrLeafPartitionQuery fromJson(String json) {
        return querySerDe.fromJsonOrLeafQuery(json);
    }
}
