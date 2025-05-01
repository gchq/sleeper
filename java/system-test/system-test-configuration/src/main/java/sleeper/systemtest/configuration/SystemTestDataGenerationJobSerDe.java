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
package sleeper.systemtest.configuration;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Serialises a data generation job to and from a JSON string.
 */
public class SystemTestDataGenerationJobSerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrinting;

    public SystemTestDataGenerationJobSerDe() {
        this.gson = new GsonBuilder().create();
        this.gsonPrettyPrinting = new GsonBuilder()
                .setPrettyPrinting()
                .create();
    }

    /**
     * Serialise a data generation job to JSON.
     *
     * @param  job the data generation job
     * @return     a JSON representation of the job
     */
    public String toJson(SystemTestDataGenerationJob job) {
        return gson.toJson(job);
    }

    /**
     * Serialise a data generation job job to JSON.
     *
     * @param  job         the data generation job
     * @param  prettyPrint true if the JSON should be formatted for readability
     * @return             a JSON representation of the job
     */
    public String toJson(SystemTestDataGenerationJob job, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrinting.toJson(job);
        }
        return toJson(job);
    }

    /**
     * Deserialises a JSON string to a data generation job.
     *
     * @param  jsonString the JSON string
     * @return            the job
     */
    public SystemTestDataGenerationJob fromJson(String jsonString) {
        return gson.fromJson(jsonString, SystemTestDataGenerationJob.class);
    }
}
