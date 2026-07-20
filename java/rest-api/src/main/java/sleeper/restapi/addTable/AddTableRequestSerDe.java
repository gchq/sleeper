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
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.serialiser.FieldValueJsonSerDe;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Properties;

/**
 * Serialises a request to add a Sleeper table to and from JSON.
 */
public class AddTableRequestSerDe {

    private final Gson gson;
    private final Gson gsonPrettyPrint;

    public AddTableRequestSerDe(InstanceProperties instanceProperties) {
        GsonBuilder builder = SchemaSerDe.registerTypeAdapters(new GsonBuilder())
                .registerTypeAdapter(AddTableRequest.class, new AddTableRequestSerialiser())
                .registerTypeAdapter(AddTableRequest.class, new AddTableRequestDeserialiser(instanceProperties));
        gson = builder.create();
        gsonPrettyPrint = builder.setPrettyPrinting().create();
    }

    /**
     * Serialises to a JSON string.
     *
     * @param  request the request
     * @return         a JSON representation of the request
     */
    public String toJson(AddTableRequest request) {
        return gson.toJson(request);
    }

    /**
     * Serialises to a JSON string.
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
     * Deserialises a JSON string.
     *
     * @param  json the JSON string
     * @return      the parsed request
     */
    public AddTableRequest fromJson(String json) {
        return gson.fromJson(json, AddTableRequest.class).validate();
    }

    /**
     * A GSON plugin to serialise a request to add a table.
     */
    private static class AddTableRequestSerialiser implements JsonSerializer<AddTableRequest> {

        @Override
        public JsonElement serialize(AddTableRequest src, Type typeOfSrc, JsonSerializationContext context) {
            JsonObject object = new JsonObject();
            TableProperties properties = src.getProperties();
            object.add("properties", context.serialize(properties.toMapExcludingSchema()));
            object.add("schema", context.serialize(properties.getSchema()));
            Field rowKey = properties.getSchema().getRowKeyFields().get(0);
            JsonArray splitPoints = new JsonArray();
            src.getSplitPoints().stream()
                    .map(value -> FieldValueJsonSerDe.toJsonElement(rowKey, value))
                    .forEach(splitPoints::add);
            object.add("splitPoints", splitPoints);
            return object;
        }
    }

    /**
     * A GSON plugin to deserialise a request to add a table.
     */
    private static class AddTableRequestDeserialiser implements JsonDeserializer<AddTableRequest> {

        private final InstanceProperties instanceProperties;

        AddTableRequestDeserialiser(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
        }

        @Override
        public AddTableRequest deserialize(JsonElement jsonElement, Type typeOfSrc, JsonDeserializationContext context) throws JsonParseException {
            JsonObject object = jsonElement.getAsJsonObject();
            TableProperties tableProperties = readTableProperties(instanceProperties, object, context);
            return AddTableRequest.builder()
                    .properties(tableProperties)
                    .splitPoints(readSplitPoints(tableProperties, object, context))
                    .build();
        }
    }

    private static TableProperties readTableProperties(InstanceProperties instanceProperties, JsonObject object, JsonDeserializationContext context) {
        Properties properties = context.deserialize(object.get("properties"), Properties.class);
        Schema schema = context.deserialize(object.get("schema"), Schema.class);
        if (properties == null) {
            return null;
        }
        TableProperties tableProperties = new TableProperties(instanceProperties, properties);
        tableProperties.setSchema(schema);
        return tableProperties;
    }

    private static List<Object> readSplitPoints(TableProperties tableProperties, JsonObject object, JsonDeserializationContext context) throws JsonParseException {
        if (tableProperties == null) {
            return null;
        }
        List<JsonElement> splitPointElements = context.deserialize(object.get("splitPoints"),
                new TypeToken<List<JsonElement>>() {
                }.getType());
        if (splitPointElements == null) {
            return null;
        }
        Field rowKey = tableProperties.getSchema().getRowKeyFields().get(0);
        return splitPointElements.stream()
                .map(element -> FieldValueJsonSerDe.fromJsonElement(rowKey, element))
                .toList();
    }
}
