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
import sleeper.core.statestore.commit.CommitRequestType;
import sleeper.core.util.GsonConfig;

public class CompactionJobIdAssignmentCommitRequestSerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrint;

    public CompactionJobIdAssignmentCommitRequestSerDe() {
        GsonBuilder builder = GsonConfig.standardBuilder()
                .registerTypeAdapter(CompactionJob.class, new CompactionJobJsonSerDe())
                .serializeNulls();
        gson = builder.create();
        gsonPrettyPrint = builder.setPrettyPrinting().create();
    }

    public String toJson(CompactionJobIdAssignmentCommitRequest compactionJobCommitRequest) {
        return gson.toJson(new WrappedCommitRequest(compactionJobCommitRequest), WrappedCommitRequest.class);
    }

    public String toJsonPrettyPrint(CompactionJobIdAssignmentCommitRequest compactionJobCommitRequest) {
        return gsonPrettyPrint.toJson(new WrappedCommitRequest(compactionJobCommitRequest), WrappedCommitRequest.class);
    }

    public CompactionJobIdAssignmentCommitRequest fromJson(String json) {
        WrappedCommitRequest wrappedRequest = gson.fromJson(json, WrappedCommitRequest.class);
        if (CommitRequestType.COMPACTION_JOB_ID_ASSIGNMENT == wrappedRequest.type) {
            return wrappedRequest.request;
        }
        throw new IllegalArgumentException("Unexpected request type");
    }

    private static class WrappedCommitRequest {
        private final CommitRequestType type;
        private final CompactionJobIdAssignmentCommitRequest request;

        WrappedCommitRequest(CompactionJobIdAssignmentCommitRequest request) {
            this.type = CommitRequestType.COMPACTION_JOB_ID_ASSIGNMENT;
            this.request = request;
        }
    }
}
