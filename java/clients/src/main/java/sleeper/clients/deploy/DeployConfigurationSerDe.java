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
package sleeper.clients.deploy;

import com.google.common.base.CaseFormat;
import com.google.gson.Gson;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import sleeper.clients.deploy.container.DockerImageLocation;
import sleeper.core.util.GsonConfig;

import java.lang.reflect.Type;

public class DeployConfigurationSerDe {
    private final Gson gson;

    public DeployConfigurationSerDe() {
        gson = GsonConfig.standardBuilder()
                .registerTypeAdapter(DockerImageLocation.class, new DockerImageLocationSerDe())
                .setPrettyPrinting().create();
    }

    public String toJson(DeployConfiguration configuration) {
        return gson.toJson(configuration);
    }

    public DeployConfiguration fromJson(String json) {
        return gson.fromJson(json, DeployConfiguration.class);
    }

    private static class DockerImageLocationSerDe implements JsonSerializer<DockerImageLocation>, JsonDeserializer<DockerImageLocation> {

        @Override
        public DockerImageLocation deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            String string = json.getAsString();
            for (DockerImageLocation location : DockerImageLocation.values()) {
                if (getJsonName(location).equalsIgnoreCase(string)) {
                    return location;
                }
            }
            throw new IllegalArgumentException("Unrecognised Docker image location: " + string);
        }

        @Override
        public JsonElement serialize(DockerImageLocation src, Type typeOfSrc, JsonSerializationContext context) {
            return new JsonPrimitive(getJsonName(src));
        }

    }

    private static String getJsonName(DockerImageLocation location) {
        return CaseFormat.UPPER_UNDERSCORE
                .converterTo(CaseFormat.LOWER_CAMEL)
                .convert(location.name());
    }

}
