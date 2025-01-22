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
package sleeper.core.statestore.commit;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.transactionlog.transactions.TransactionSerDe;
import sleeper.core.statestore.transactionlog.transactions.TransactionType;
import sleeper.core.util.GsonConfig;

import java.lang.reflect.Type;
import java.util.Objects;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * Serialises and deserialises a commit request for a transaction to be added to the state store.
 */
public class StateStoreCommitRequestSerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrint;

    public StateStoreCommitRequestSerDe(TablePropertiesProvider tablePropertiesProvider) {
        this(tableId -> new TransactionSerDe(tablePropertiesProvider.getById(tableId).getSchema()));
    }

    public StateStoreCommitRequestSerDe(TableProperties tableProperties) {
        this(tableProperties.get(TABLE_ID), new TransactionSerDe(tableProperties.getSchema()));
    }

    private StateStoreCommitRequestSerDe(String expectedTableId, TransactionSerDe serDe) {
        this(tableId -> {
            if (Objects.equals(tableId, expectedTableId)) {
                return serDe;
            } else {
                throw new IllegalArgumentException("Expected table ID " + expectedTableId + ", found " + tableId);
            }
        });
    }

    private StateStoreCommitRequestSerDe(TransactionSerDeProvider transactionSerDeProvider) {
        GsonBuilder builder = GsonConfig.standardBuilder()
                .registerTypeAdapter(StateStoreCommitRequest.class, new TransactionByTypeJsonSerDe(transactionSerDeProvider));
        gson = builder.create();
        gsonPrettyPrint = builder.setPrettyPrinting().create();
    }

    /**
     * Creates an instance of this class that only supports file transactions. The table properties are not required.
     *
     * @return the serialiser
     */
    public static StateStoreCommitRequestSerDe forFileTransactions() {
        TransactionSerDe serDe = TransactionSerDe.forFileTransactions();
        return new StateStoreCommitRequestSerDe(tableId -> serDe);
    }

    /**
     * Serialises a commit request to a JSON string.
     *
     * @param  request the commit request
     * @return         the JSON string
     */
    public String toJson(StateStoreCommitRequest request) {
        return gson.toJson(request);
    }

    /**
     * Serialises a commit request to a pretty-printed JSON string.
     *
     * @param  request the commit request
     * @return         the pretty-printed JSON string
     */
    public String toJsonPrettyPrint(StateStoreCommitRequest request) {
        return gsonPrettyPrint.toJson(request);
    }

    /**
     * Deserialises a commit request from a JSON string.
     *
     * @param  json the JSON string
     * @return      the commit request
     */
    public StateStoreCommitRequest fromJson(String json) {
        return gson.fromJson(json, StateStoreCommitRequest.class);
    }

    /**
     * A GSON plugin to serialise/deserialise a request for a transaction, serialising a transaction by its type.
     */
    private static class TransactionByTypeJsonSerDe implements JsonSerializer<StateStoreCommitRequest>, JsonDeserializer<StateStoreCommitRequest> {
        private final TransactionSerDeProvider transactionSerDeProvider;

        private TransactionByTypeJsonSerDe(TransactionSerDeProvider transactionSerDeProvider) {
            this.transactionSerDeProvider = transactionSerDeProvider;
        }

        @Override
        public StateStoreCommitRequest deserialize(JsonElement jsonElement, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            if (!jsonElement.isJsonObject()) {
                throw new JsonParseException("Expected JsonObject, got " + jsonElement);
            }
            JsonObject json = jsonElement.getAsJsonObject();
            String tableId = json.get("tableId").getAsString();
            TransactionType transactionType = context.deserialize(json.get("transactionType"), TransactionType.class);
            JsonElement bodyKeyElement = json.get("bodyKey");
            if (bodyKeyElement != null) {
                return StateStoreCommitRequest.create(tableId, bodyKeyElement.getAsString(), transactionType);
            } else {
                return StateStoreCommitRequest.create(tableId,
                        transactionSerDeProvider.getByTableId(tableId)
                                .toTransaction(transactionType, json.get("transaction")));
            }
        }

        @Override
        public JsonElement serialize(StateStoreCommitRequest request, Type typeOfSrc, JsonSerializationContext context) {
            JsonObject json = new JsonObject();
            json.addProperty("tableId", request.getTableId());
            json.add("transactionType", context.serialize(request.getTransactionType()));
            request.getTransactionIfHeld().ifPresentOrElse(transaction -> {
                json.add("transaction",
                        transactionSerDeProvider.getByTableId(request.getTableId())
                                .toJsonTree(transaction));
            }, () -> {
                json.addProperty("bodyKey", request.getBodyKey());
            });
            return json;
        }
    }

    /**
     * A way to retrieve a transaction serialiser by the table ID.
     */
    private interface TransactionSerDeProvider {
        /**
         * Gets the properties of a Sleeper table.
         *
         * @param  tableId the table ID
         * @return         the properties
         */
        TransactionSerDe getByTableId(String tableId);
    }
}
