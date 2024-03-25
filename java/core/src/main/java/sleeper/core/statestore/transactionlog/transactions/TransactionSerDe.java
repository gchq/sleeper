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

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionSerDe.PartitionJsonSerDe;
import sleeper.core.schema.Schema;
import sleeper.core.util.GsonConfig;

public class TransactionSerDe {
    private final Gson gson;

    public TransactionSerDe(Schema schema) {
        GsonBuilder builder = GsonConfig.standardBuilder()
                .registerTypeAdapter(Partition.class, new PartitionJsonSerDe(schema))
                .serializeNulls();
        this.gson = builder.create();
    }

    public String toJson(Object transaction) {
        return gson.toJson(transaction);
    }

    public <T> T toTransaction(Class<T> type, String json) {
        return gson.fromJson(json, type);
    }
}
