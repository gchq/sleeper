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
package sleeper.commit;

import com.google.gson.Gson;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobJsonSerDe;
import sleeper.core.util.GsonConfig;

/**
 * Deserialises a state store commit request.
 */
public class StateStoreCommitRequestDeserialiser {
    private final Gson gson = GsonConfig.standardBuilder()
            .registerTypeAdapter(CompactionJob.class, new CompactionJobJsonSerDe())
            .serializeNulls()
            .create();

    /**
     * Deserialises a state store commit request.
     *
     * @param  jsonString the JSON string
     * @return            a commit request
     */
    public StateStoreCommitRequest fromJson(String jsonString) {
        StateStoreCommitRequestJson commitRequestJson = gson.fromJson(jsonString, StateStoreCommitRequestJson.class);
        return commitRequestJson.getCommitRequest();
    }
}
