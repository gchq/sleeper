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
package sleeper.ingest.job.commit;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import sleeper.core.statestore.CommitRequestType;
import sleeper.core.util.GsonConfig;

/**
 * Serialises and deserialises an ingest job commit request to and from JSON.
 */
public class IngestJobCommitRequestSerDe {

    private final Gson gson;
    private final Gson gsonPrettyPrint;

    public IngestJobCommitRequestSerDe() {
        GsonBuilder builder = GsonConfig.standardBuilder()
                .serializeNulls();
        gson = builder.create();
        gsonPrettyPrint = builder.setPrettyPrinting().create();
    }

    /**
     * Serialises an ingest job commit request to a JSON string.
     *
     * @param  request the ingest job commit request
     * @return         the JSON string
     */
    public String toJson(IngestJobCommitRequest request) {
        return gson.toJson(new WrappedCommitRequest(request), WrappedCommitRequest.class);
    }

    /**
     * Serialises an ingest job commit request to a pretty-printed JSON string.
     *
     * @param  request the ingest job commit request
     * @return         the pretty-printed JSON string
     */
    public String toJsonPrettyPrint(IngestJobCommitRequest request) {
        return gsonPrettyPrint.toJson(new WrappedCommitRequest(request), WrappedCommitRequest.class);
    }

    /**
     * Deserialises an ingest job commit request from a JSON string.
     *
     * @param  json the JSON string
     * @return      the ingest job commit request
     */
    public IngestJobCommitRequest fromJson(String json) {
        WrappedCommitRequest wrappedRequest = gson.fromJson(json, WrappedCommitRequest.class);
        if (CommitRequestType.INGEST_ADD_FILES == wrappedRequest.type) {
            return wrappedRequest.request;
        }
        return null;
    }

    /**
     * Stores an ingest job commit request with the type of commit request. Used by the state store committer to
     * deserialise the correct commit request.
     */
    private static class WrappedCommitRequest {
        private CommitRequestType type;
        private IngestJobCommitRequest request;

        WrappedCommitRequest(IngestJobCommitRequest request) {
            this.type = CommitRequestType.INGEST_ADD_FILES;
            this.request = request;
        }
    }
}
