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

/**
 * Reads queries from messages on a query queue, loading the query from a store if the message points to one. This is
 * the counterpart to {@link QueryMessageUploader}.
 */
public class QueryMessageReader {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryMessageReader.class);

    private final QueryMessageSerDe messageSerDe;
    private final QueryJsonStore jsonStore;

    public QueryMessageReader(QueryJsonStore jsonStore, QuerySerDe querySerDe) {
        this(jsonStore, new QueryMessageSerDe(querySerDe));
    }

    public QueryMessageReader(QueryJsonStore jsonStore, QueryMessageSerDe messageSerDe) {
        this.messageSerDe = messageSerDe;
        this.jsonStore = jsonStore;
    }

    /**
     * Reads a query from a message, loading it from the store if the message points to a stored query.
     *
     * @param  message the message body
     * @return         the query
     */
    public QueryOrLeafPartitionQuery readAndLoadIfStored(String message) {
        String json = messageSerDe.readStoredQueryKey(message)
                .map(key -> {
                    LOGGER.info("Loading query JSON from {}", key);
                    return jsonStore.load(key);
                })
                .orElse(message);
        return messageSerDe.fromJson(json);
    }
}
