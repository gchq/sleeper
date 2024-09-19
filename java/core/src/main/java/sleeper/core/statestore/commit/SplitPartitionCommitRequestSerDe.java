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

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionSerDe.PartitionJsonSerDe;
import sleeper.core.schema.Schema;
import sleeper.core.util.GsonConfig;

/**
 * Serialises and deserialises a commit request to split a partition.
 */
public class SplitPartitionCommitRequestSerDe {

    private final Gson gson;
    private final Gson gsonPrettyPrint;

    public SplitPartitionCommitRequestSerDe(Schema schema) {
        GsonBuilder builder = GsonConfig.standardBuilder()
                .registerTypeAdapter(Partition.class, new PartitionJsonSerDe(schema))
                .serializeNulls();
        gson = builder.create();
        gsonPrettyPrint = builder.setPrettyPrinting().create();
    }

    /**
     * Serialises a split partition commit request to a JSON string.
     *
     * @param  request the commit request
     * @return         the JSON string
     */
    public String toJson(SplitPartitionCommitRequest request) {
        return gson.toJson(new WrappedCommitRequest(request), WrappedCommitRequest.class);
    }

    /**
     * Serialises a split partition commit request to a pretty-printed JSON string.
     *
     * @param  request the commit request
     * @return         the pretty-printed JSON string
     */
    public String toJsonPrettyPrint(SplitPartitionCommitRequest request) {
        return gsonPrettyPrint.toJson(new WrappedCommitRequest(request), WrappedCommitRequest.class);
    }

    /**
     * Deserialises a split partition commit request from a JSON string.
     *
     * @param  json the JSON string
     * @return      the commit request
     */
    public SplitPartitionCommitRequest fromJson(String json) {
        WrappedCommitRequest wrappedRequest = gson.fromJson(json, WrappedCommitRequest.class);
        if (CommitRequestType.SPLIT_PARTITION == wrappedRequest.type) {
            return wrappedRequest.request;
        }
        throw new IllegalArgumentException("Unexpected request type");
    }

    /**
     * Stores a split partition commit request with the type of commit request. Used by the state store committer to
     * deserialise the correct commit request.
     */
    private static class WrappedCommitRequest {
        private final CommitRequestType type;
        private final SplitPartitionCommitRequest request;

        WrappedCommitRequest(SplitPartitionCommitRequest request) {
            this.type = CommitRequestType.SPLIT_PARTITION;
            this.request = request;
        }
    }

}
