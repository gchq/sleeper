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
package sleeper.compaction.core.job.commit;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import sleeper.core.statestore.FileReferenceSerDe;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.util.GsonConfig;

public class CompactionCommitRequestSerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrint;

    public CompactionCommitRequestSerDe() {
        GsonBuilder builder = GsonConfig.standardBuilder()
                .addSerializationExclusionStrategy(FileReferenceSerDe.excludeUpdateTimes());
        gson = builder.create();
        gsonPrettyPrint = builder.setPrettyPrinting().create();
    }

    public String toJson(String tableId, ReplaceFileReferencesRequest request) {
        return toJson(new CompactionCommitMessage(tableId, request));
    }

    public String toJson(CompactionCommitMessage message) {
        return gson.toJson(message);
    }

    public String toJsonPrettyPrint(String tableId, ReplaceFileReferencesRequest request) {
        return gsonPrettyPrint.toJson(new CompactionCommitMessage(tableId, request));
    }

    public CompactionCommitRequest fromJsonWithCallbackOnFail(String json, Runnable callbackOnFail) {
        CompactionCommitRequest jsonRequest = gson.fromJson(json, CompactionCommitRequest.class);
        return new CompactionCommitRequest(jsonRequest.tableId(), jsonRequest.request(), callbackOnFail);
    }

}
