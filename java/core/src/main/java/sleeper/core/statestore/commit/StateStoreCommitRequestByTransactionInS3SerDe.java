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

import sleeper.core.util.GsonConfig;

/**
 * Serialises and deserialises a commit request for a transaction that was uploaded to S3 before being submitted to the
 * committer.
 */
public class StateStoreCommitRequestByTransactionInS3SerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrint;

    public StateStoreCommitRequestByTransactionInS3SerDe() {
        GsonBuilder builder = GsonConfig.standardBuilder();
        gson = builder.create();
        gsonPrettyPrint = builder.setPrettyPrinting().create();
    }

    /**
     * Serialises a commit request to a pretty-printed JSON string.
     *
     * @param  request the commit request
     * @return         the pretty-printed JSON string
     */
    public String toJsonPrettyPrint(StateStoreCommitRequestByTransactionInS3 request) {
        return gsonPrettyPrint.toJson(request);
    }

    /**
     * Deserialises a commit request from a JSON string.
     *
     * @param  json the JSON string
     * @return      the commit request
     */
    public StateStoreCommitRequestByTransactionInS3 fromJson(String json) {
        return gson.fromJson(json, StateStoreCommitRequestByTransactionInS3.class);
    }

}
