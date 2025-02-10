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
package sleeper.core.statestore.transactionlog.transactions;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionSerDe.PartitionJsonSerDe;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.AllReferencesToAFileSerDe;
import sleeper.core.statestore.FileReferenceSerDe;
import sleeper.core.util.GsonConfig;
import sleeper.core.util.RefuseTypeJsonSerDe;

/**
 * Serialises and deserialises transactions to and from JSON. This can be used to store the transactions in a log.
 */
public class TransactionSerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrint;

    public TransactionSerDe(Schema schema) {
        this(new PartitionJsonSerDe(schema));
    }

    private TransactionSerDe(Object partitionTypeAdapter) {
        GsonBuilder builder = GsonConfig.standardBuilder()
                .registerTypeAdapter(Partition.class, partitionTypeAdapter)
                .registerTypeAdapter(AllReferencesToAFile.class, AllReferencesToAFileSerDe.noUpdateTimes())
                .addSerializationExclusionStrategy(FileReferenceSerDe.excludeUpdateTimes());
        gson = builder.create();
        gsonPrettyPrint = builder.setPrettyPrinting().create();
    }

    /**
     * Creates an instance of the SerDe that only supports file transactions. The schema is not required.
     *
     * @return the SerDe
     */
    public static TransactionSerDe forFileTransactions() {
        return new TransactionSerDe(new RefuseTypeJsonSerDe<Partition>());
    }

    /**
     * Serialises a transaction to JSON.
     *
     * @param  transaction the transaction
     * @return             the JSON
     */
    public String toJson(StateStoreTransaction<?> transaction) {
        return gson.toJson(transaction);
    }

    /**
     * Serialises a transaction to JSON. Formats the JSON to be human-readable.
     *
     * @param  transaction the transaction
     * @return             the JSON
     */
    public String toJsonPrettyPrint(StateStoreTransaction<?> transaction) {
        return gsonPrettyPrint.toJson(transaction);
    }

    /**
     * Serialises a transaction to JSON.
     *
     * @param  transaction the transaction
     * @return             the JSON
     */
    public JsonElement toJsonTree(StateStoreTransaction<?> transaction) {
        return gson.toJsonTree(transaction);
    }

    /**
     * Deserialises a transaction from JSON.
     *
     * @param  type the type of transaction (expected to be held in the log entry)
     * @param  json the JSON
     * @return      the transaction
     */
    public StateStoreTransaction<?> toTransaction(TransactionType type, String json) {
        return gson.fromJson(json, type.getType());
    }

    /**
     * Deserialises a transaction from JSON.
     *
     * @param  type the type of transaction (expected to be held in the log entry)
     * @param  json the JSON
     * @return      the transaction
     */
    public StateStoreTransaction<?> toTransaction(TransactionType type, JsonElement json) {
        return gson.fromJson(json, type.getType());
    }
}
