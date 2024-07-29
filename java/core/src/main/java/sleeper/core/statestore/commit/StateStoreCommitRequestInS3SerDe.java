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

import sleeper.core.statestore.FileReferenceSerDe;
import sleeper.core.util.GsonConfig;

/**
 * Serialises and deserialises a commit request stored in S3.
 */
public class StateStoreCommitRequestInS3SerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrint;

    public StateStoreCommitRequestInS3SerDe() {
        GsonBuilder builder = GsonConfig.standardBuilder()
                .addSerializationExclusionStrategy(FileReferenceSerDe.excludeUpdateTimes());
        gson = builder.create();
        gsonPrettyPrint = builder.setPrettyPrinting().create();
    }

    /**
     * Serialises a commit request stored in S3 to a JSON string.
     *
     * @param  request the commit request
     * @return         the JSON string
     */
    public String toJson(StateStoreCommitRequestInS3 request) {
        return gson.toJson(new WrappedCommitRequest(request), WrappedCommitRequest.class);
    }

    /**
     * Serialises a commit request stored in S3 to a pretty-printed JSON string.
     *
     * @param  request the commit request
     * @return         the pretty-printed JSON string
     */
    public String toJsonPrettyPrint(StateStoreCommitRequestInS3 request) {
        return gsonPrettyPrint.toJson(new WrappedCommitRequest(request), WrappedCommitRequest.class);
    }

    /**
     * Deserialises a commit request stored in S3 from a JSON string.
     *
     * @param  json the JSON string
     * @return      the commit request
     */
    public StateStoreCommitRequestInS3 fromJson(String json) {
        WrappedCommitRequest wrappedRequest = gson.fromJson(json, WrappedCommitRequest.class);
        if (CommitRequestType.STORED_IN_S3 == wrappedRequest.type) {
            return wrappedRequest.request;
        }
        throw new IllegalArgumentException("Unexpected request type");
    }

    /**
     * Stores a commit request stored in S3 with the type of commit request. Used by the state store committer to
     * deserialise the correct commit request.
     */
    private static class WrappedCommitRequest {
        private final CommitRequestType type;
        private final StateStoreCommitRequestInS3 request;

        WrappedCommitRequest(StateStoreCommitRequestInS3 request) {
            this.type = CommitRequestType.STORED_IN_S3;
            this.request = request;
        }
    }
}
