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
package sleeper.compaction.job.commit;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobJsonSerDe;
import sleeper.core.statestore.CommitRequestType;
import sleeper.core.util.GsonConfig;

public class CompactionJobCommitRequestSerDe {

    private final Gson gson;
    private final Gson gsonPrettyPrint;

    public CompactionJobCommitRequestSerDe() {
        GsonBuilder builder = GsonConfig.standardBuilder()
                .registerTypeAdapter(CompactionJob.class, new CompactionJobJsonSerDe())
                .serializeNulls();
        gson = builder.create();
        gsonPrettyPrint = builder.setPrettyPrinting().create();
    }

    public String toJson(CompactionJobCommitRequest compactionJobCommitRequest) {
        return gson.toJson(new WrappedCommitRequest(compactionJobCommitRequest), WrappedCommitRequest.class);
    }

    public String toJsonPrettyPrint(CompactionJobCommitRequest compactionJobCommitRequest) {
        return gsonPrettyPrint.toJson(new WrappedCommitRequest(compactionJobCommitRequest), WrappedCommitRequest.class);
    }

    public CompactionJobCommitRequest fromJson(String json) {
        WrappedCommitRequest wrappedRequest = gson.fromJson(json, WrappedCommitRequest.class);
        if (CommitRequestType.COMPACTION_FINISHED == wrappedRequest.type) {
            return wrappedRequest.request;
        }
        throw new IllegalArgumentException("Unexpected request type");
    }

    private static class WrappedCommitRequest {
        private final CommitRequestType type;
        private final CompactionJobCommitRequest request;

        WrappedCommitRequest(CompactionJobCommitRequest request) {
            this.type = CommitRequestType.COMPACTION_FINISHED;
            this.request = request;
        }
    }
}
