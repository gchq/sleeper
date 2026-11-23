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

import java.util.UUID;
import java.util.function.Supplier;

/**
 * Serialises queries to messages for a query queue, uploading the query to a store if it is too large to fit in a
 * message. If the message is too large then the message will contain the location of the serialised query.
 */
public class QueryMessageUploader {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryMessageUploader.class);

    /**
     * Minimum length of a JSON string that will be held in the store rather than the message.
     * <p>
     * Max size of an SQS message is 256KiB. Setting this to less than 256KiB leaves room for message attributes.
     * https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html
     */
    public static final int DEFAULT_JSON_LENGTH_TO_STORE = 1024 * 200;

    private final QueryMessageSerDe messageSerDe;
    private final QueryJsonStore jsonStore;
    private final int jsonLengthToStore;
    private final Supplier<String> uuidSupplier;

    public QueryMessageUploader(QueryJsonStore jsonStore, QuerySerDe querySerDe) {
        this(jsonStore, new QueryMessageSerDe(querySerDe), DEFAULT_JSON_LENGTH_TO_STORE, () -> UUID.randomUUID().toString());
    }

    public QueryMessageUploader(QueryJsonStore jsonStore, QueryMessageSerDe messageSerDe, int jsonLengthToStore, Supplier<String> uuidSupplier) {
        this.messageSerDe = messageSerDe;
        this.jsonStore = jsonStore;
        this.jsonLengthToStore = jsonLengthToStore;
        this.uuidSupplier = uuidSupplier;
    }

    /**
     * Serialises a query to a message, uploading it to the store if it is too big.
     *
     * @param  query the query
     * @return       the message body
     */
    public String serialiseAndUploadIfTooBig(Query query) {
        return uploadIfTooBig(messageSerDe.toJson(query), query.getQueryId(), "query");
    }

    /**
     * Serialises a leaf partition query to a message, uploading it to the store if it is too big.
     *
     * @param  query the leaf partition query
     * @return       the message body
     */
    public String serialiseAndUploadIfTooBig(LeafPartitionQuery query) {
        return uploadIfTooBig(messageSerDe.toJson(query), query.getQueryId(), query.getSubQueryId());
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

    private String uploadIfTooBig(String json, String queryId, String subQueryId) {
        if (json.length() < jsonLengthToStore) {
            return json;
        }
        String key = createKey(queryId, subQueryId, uuidSupplier.get());
        LOGGER.info("Query JSON of length {} is too large for a message, uploading to {}", json.length(), key);
        jsonStore.store(key, json);
        return messageSerDe.toJsonPointingToStoredQuery(queryId, key);
    }
}
