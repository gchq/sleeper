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
package sleeper.compaction.core.job.dispatch;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import sleeper.core.util.GsonConfig;

public class CompactionJobDispatchRequestSerDe {

    private final Gson gson;
    private final Gson gsonPrettyPrint;

    public CompactionJobDispatchRequestSerDe() {
        GsonBuilder builder = GsonConfig.standardBuilder();
        gson = builder.create();
        gsonPrettyPrint = builder.setPrettyPrinting().create();
    }

    public String toJsonPrettyPrint(CompactionJobDispatchRequest job) {
        return gsonPrettyPrint.toJson(job);
    }

    public String toJson(CompactionJobDispatchRequest job) {
        return gson.toJson(job);
    }

    public CompactionJobDispatchRequest fromJson(String json) {
        return gson.fromJson(json, CompactionJobDispatchRequest.class);
    }

}
