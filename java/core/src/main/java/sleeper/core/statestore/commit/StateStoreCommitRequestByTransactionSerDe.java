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
public class StateStoreCommitRequestByTransactionSerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrint;

    public StateStoreCommitRequestByTransactionSerDe(TablePropertiesProvider tablePropertiesProvider) {
        this(tablePropertiesProvider::getById);
    }

    public StateStoreCommitRequestByTransactionSerDe(TableProperties tableProperties) {
        this(tableId -> {
            if (Objects.equals(tableId, tableProperties.get(TABLE_ID))) {
                return tableProperties;
            } else {
                throw new IllegalArgumentException("Expected table ID " + tableProperties.get(TABLE_ID) + ", found " + tableId);
            }
        });
    }

    private StateStoreCommitRequestByTransactionSerDe(TablePropertiesSource tablePropertiesProvider) {
        GsonBuilder builder = GsonConfig.standardBuilder()
                .registerTypeAdapter(StateStoreCommitRequestByTransaction.class, new TransactionByTypeJsonSerDe(tablePropertiesProvider));
        gson = builder.create();
        gsonPrettyPrint = builder.setPrettyPrinting().create();
    }

    /**
     * Serialises a commit request to a JSON string.
     *
     * @param  request the commit request
     * @return         the JSON string
     */
    public String toJson(StateStoreCommitRequestByTransaction request) {
        return gson.toJson(request);
    }

    /**
     * Serialises a commit request to a pretty-printed JSON string.
     *
     * @param  request the commit request
     * @return         the pretty-printed JSON string
     */
    public String toJsonPrettyPrint(StateStoreCommitRequestByTransaction request) {
        return gsonPrettyPrint.toJson(request);
    }

    /**
     * Deserialises a commit request from a JSON string.
     *
     * @param  json the JSON string
     * @return      the commit request
     */
    public StateStoreCommitRequestByTransaction fromJson(String json) {
        return gson.fromJson(json, StateStoreCommitRequestByTransaction.class);
    }

    /**
     * A GSON plugin to serialise/deserialise a request for a transaction, serialising a transaction by its type.
     */
    public static class TransactionByTypeJsonSerDe implements JsonSerializer<StateStoreCommitRequestByTransaction>, JsonDeserializer<StateStoreCommitRequestByTransaction> {
        private final TablePropertiesSource tablePropertiesProvider;

        public TransactionByTypeJsonSerDe(TablePropertiesSource tablePropertiesProvider) {
            this.tablePropertiesProvider = tablePropertiesProvider;
        }

        @Override
        public StateStoreCommitRequestByTransaction deserialize(JsonElement jsonElement, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            if (!jsonElement.isJsonObject()) {
                throw new JsonParseException("Expected JsonObject, got " + jsonElement);
            }
            JsonObject json = jsonElement.getAsJsonObject();
            String tableId = json.get("tableId").getAsString();
            TransactionType transactionType = context.deserialize(json.get("transactionType"), TransactionType.class);
            JsonElement bodyKeyElement = json.get("bodyKey");
            if (bodyKeyElement != null) {
                return StateStoreCommitRequestByTransaction.create(tableId, bodyKeyElement.getAsString(), transactionType);
            } else {
                return StateStoreCommitRequestByTransaction.create(tableId,
                        transactionSerDe(tableId).toTransaction(transactionType, json.get("transaction")));
            }
        }

        @Override
        public JsonElement serialize(StateStoreCommitRequestByTransaction request, Type typeOfSrc, JsonSerializationContext context) {
            JsonObject json = new JsonObject();
            json.addProperty("tableId", request.getTableId());
            json.add("transactionType", context.serialize(request.getTransactionType()));
            request.getTransactionIfHeld().ifPresentOrElse(transaction -> {
                json.add("transaction", transactionSerDe(request.getTableId()).toJsonTree(transaction));
            }, () -> {
                json.addProperty("bodyKey", request.getBodyKey());
            });
            return json;
        }

        private TransactionSerDe transactionSerDe(String tableId) {
            TableProperties tableProperties = tablePropertiesProvider.getById(tableId);
            return new TransactionSerDe(tableProperties.getSchema());
        }
    }

    /**
     * A way to retrieve table properties by the table ID.
     */
    public interface TablePropertiesSource {
        /**
         * Gets the properties of a Sleeper table.
         *
         * @param  tableId the table ID
         * @return         the properties
         */
        TableProperties getById(String tableId);
    }
}
