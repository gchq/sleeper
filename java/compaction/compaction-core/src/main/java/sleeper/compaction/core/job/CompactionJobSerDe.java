/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.compaction.core.job;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import sleeper.core.util.GsonConfig;

import java.util.List;

/**
 * Serialises and deserialises a compaction job to and from a JSON string.
 */
public class CompactionJobSerDe {

    private final Gson gson;
    private final Gson gsonPrettyPrint;

    public CompactionJobSerDe() {
        GsonBuilder builder = GsonConfig.standardBuilder();
        gson = builder.create();
        gsonPrettyPrint = builder.setPrettyPrinting().create();
    }

    public String toJsonPrettyPrint(CompactionJob job) {
        return gsonPrettyPrint.toJson(job);
    }

    public String toJson(CompactionJob job) {
        return gson.toJson(job);
    }

    public String toJsonPrettyPrint(List<CompactionJob> batch) {
        return gsonPrettyPrint.toJson(new Batch(batch));
    }

    public String toJson(List<CompactionJob> batch) {
        return gson.toJson(new Batch(batch));
    }

    public CompactionJob fromJson(String json) {
        return gson.fromJson(json, CompactionJob.class);
    }

    public List<CompactionJob> batchFromJson(String json) {
        return gson.fromJson(json, Batch.class).jobs();
    }

    private record Batch(List<CompactionJob> jobs) {
    }

}
