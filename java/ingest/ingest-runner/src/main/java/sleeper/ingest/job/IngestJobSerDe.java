/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.ingest.job;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class IngestJobSerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrinting;

    public IngestJobSerDe() {
        this.gson = new GsonBuilder().create();
        this.gsonPrettyPrinting = new GsonBuilder()
                .setPrettyPrinting()
                .create();
    }

    public String toJson(IngestJob ingestJob) {
        return gson.toJson(ingestJob);
    }

    public String toJson(IngestJob ingestJob, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrinting.toJson(ingestJob);
        }
        return toJson(ingestJob);
    }

    public IngestJob fromJson(String jsonIngestJob) {
        return gson.fromJson(jsonIngestJob, IngestJob.class);
    }
}
