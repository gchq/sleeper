/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.restapi.addTable;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

import sleeper.core.properties.local.ReadSplitPoints;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;

import java.util.List;
import java.util.Map;

/**
 * Serialises an AddTable request to and from JSON.
 */
public class AddTableRequestSerDe {

    private final Gson gson;
    private final Gson gsonPrettyPrint;

    public AddTableRequestSerDe() {
        GsonBuilder builder = SchemaSerDe.registerTypeAdapters(new GsonBuilder());
        gson = builder.create();
        gsonPrettyPrint = builder.setPrettyPrinting().create();
    }

    /**
     * Serialises an AddTable request to JSON.
     *
     * @param  request the request
     * @return         a JSON representation of the request
     */
    public String toJson(AddTableRequest request) {
        return gson.toJson(request);
    }

    /**
     * Serialises an AddTable request to JSON.
     *
     * @param  request     the request
     * @param  prettyPrint true if the JSON should be formatted for readability
     * @return             a JSON representation of the request
     */
    public String toJson(AddTableRequest request, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrint.toJson(request);
        }
        return toJson(request);
    }

    /**
     * Deserialises a JSON string to an AddTable request.
     *
     * @param  json the JSON string
     * @return      the parsed request
     */
    public AddTableRequest fromJson(String json) {
        return gson.fromJson(json, AddTableRequest.class).validate();
    }

    /**
     * A GSON plugin to deserialise a field. Treats a missing "nullable" property as false.
     */
    private static class AddTableRequestDeserialiser implements JsonDeserializer<AddTableRequest> {

        @Override
        public AddTableRequest deserialize(JsonElement jsonElement, java.lang.reflect.Type typeOfSrc, JsonDeserializationContext context) throws JsonParseException {
            JsonObject object = jsonElement.getAsJsonObject();
            Map<String, String> properties = context.deserialize(object.get("properties"),
                    new TypeToken<Map<String, String>>() {
                    }.getType());
            Schema schema = context.deserialize(object.get("schema"), Schema.class);
            List<JsonElement> splitPointElements = context.deserialize(object.get("splitPoints"),
                    new TypeToken<List<JsonElement>>() {
                    }.getType());
            List<Object> splitPoints = ReadSplitPoints.fromLines(
                    splitPointElements.stream().map(JsonElement::getAsString),
                    schema, false);
            return AddTableRequest.builder().properties(properties).schema(schema).splitPoints(splitPointElements).build();
        }
    }
}
