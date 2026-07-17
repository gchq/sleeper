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
package sleeper.core.row.serialiser;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;

/**
 * Serialises and deserialises a row to and from a JSON string.
 */
public class RowJsonSerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrinting;

    public RowJsonSerDe(Schema schema) {
        this.gson = new GsonBuilder()
                .registerTypeAdapter(Row.class, new RowGsonSerialiser(schema))
                .serializeNulls()
                .create();
        this.gsonPrettyPrinting = new GsonBuilder()
                .setPrettyPrinting()
                .registerTypeAdapter(Row.class, new RowGsonSerialiser(schema))
                .serializeNulls()
                .create();
    }

    /**
     * Serialises a row to a JSON string.
     *
     * @param  row the row
     * @return     a JSON string
     */
    public String toJson(Row row) {
        return gson.toJson(row);
    }

    /**
     * Serialises a row to a JSON string.
     *
     * @param  row         the row
     * @param  prettyPrint whether to pretty-print the JSON string
     * @return             a JSON string
     */
    public String toJson(Row row, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrinting.toJson(row);
        }
        return toJson(row);
    }

    /**
     * Deserialises a JSON string to a row.
     *
     * @param  jsonSchema the JSON string
     * @return            a row
     */
    public Row fromJson(String jsonSchema) {
        return gson.fromJson(jsonSchema, Row.class);
    }

    /**
     * A GSON plugin to serialise/deserialise a row.
     */
    public static class RowGsonSerialiser implements JsonSerializer<Row>, JsonDeserializer<Row> {
        private final Schema schema;

        public RowGsonSerialiser(Schema schema) {
            this.schema = schema;
        }

        @Override
        public JsonElement serialize(Row row, java.lang.reflect.Type typeOfSrc, JsonSerializationContext context) {
            JsonObject json = new JsonObject();
            for (Field field : schema.getAllFields()) {
                Object value = row.get(field.getName());
                json.add(field.getName(), FieldValueJsonSerDe.toJsonElement(field, value));
            }
            return json;
        }

        @Override
        public Row deserialize(JsonElement jsonElement, java.lang.reflect.Type typeOfSrc, JsonDeserializationContext context) throws JsonParseException {
            if (!jsonElement.isJsonObject()) {
                throw new JsonParseException("Expected JsonObject, got " + jsonElement);
            }
            JsonObject jsonObject = jsonElement.getAsJsonObject();
            Row row = new Row();
            for (Field field : schema.getAllFields()) {
                Object value = FieldValueJsonSerDe.fromJsonElement(field, jsonObject.get(field.getName()));
                if (value == null && !field.isNullable()) {
                    continue;
                }
                row.put(field.getName(), value);
            }
            return row;
        }
    }
}
