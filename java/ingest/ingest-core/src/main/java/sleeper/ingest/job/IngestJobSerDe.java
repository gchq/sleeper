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
import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.properties.InstanceProperties;

import java.util.Optional;

import static sleeper.utils.HadoopPathUtils.expandDirectories;

public class IngestJobSerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrinting;
    private final Configuration configuration;
    private final InstanceProperties properties;

    public IngestJobSerDe() {
        this(new Configuration(), new InstanceProperties());
    }

    public IngestJobSerDe(Configuration configuration, InstanceProperties properties) {
        this.gson = new GsonBuilder().create();
        this.gsonPrettyPrinting = new GsonBuilder()
                .setPrettyPrinting()
                .create();
        this.configuration = configuration;
        this.properties = properties;
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

    public Optional<IngestJob> fromJsonExpandingDirs(String jsonIngestJob) {
        IngestJob ingestJob = fromJson(jsonIngestJob);
        return expandDirectories(ingestJob.getFiles(), configuration, properties)
                .map(files -> ingestJob.toBuilder().files(files).build());
    }
}
