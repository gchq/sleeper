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
package sleeper.core.record.serialiser;

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

import sleeper.core.record.Record;
import sleeper.core.record.ResultsBatch;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.schema.type.Type;

import java.util.ArrayList;
import java.util.List;

/**
 * Serialises and deserialises a {@link ResultsBatch} of {@link Record}s to and from a JSON {@link String}.
 */
public class JSONResultsBatchSerialiser implements ResultsBatchSerialiser {
    private final Gson gson;
    private final Gson gsonPrettyPrinting;

    public JSONResultsBatchSerialiser() {
        try {
            GsonBuilder gsonBuilder = new GsonBuilder()
                    .registerTypeAdapter(Class.forName(ResultsBatch.class.getName()), new ResultsBatchSerDe())
                    .registerTypeAdapter(Class.forName(Type.class.getName()), new SchemaSerDe.AbstractTypeJsonSerializer())
                    .registerTypeAdapter(Class.forName(Type.class.getName()), new SchemaSerDe.AbstractTypeJsonDeserializer())
                    .serializeNulls();
            this.gson = gsonBuilder
                    .create();
            this.gsonPrettyPrinting = gsonBuilder
                    .setPrettyPrinting()
                    .create();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Exception creating Gson", e);
        }
    }

    @Override
    public String serialise(ResultsBatch resultsBatch) {
        return gson.toJson(resultsBatch);
    }

    @Override
    public ResultsBatch deserialise(String json) {
        return gson.fromJson(json, ResultsBatch.class);
    }

    public String serialise(ResultsBatch resultsBatch, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrinting.toJson(resultsBatch);
        }
        return serialise(resultsBatch);
    }

    private static class ResultsBatchSerDe implements JsonSerializer<ResultsBatch>, JsonDeserializer<ResultsBatch> {
        @Override
        public ResultsBatch deserialize(JsonElement json, java.lang.reflect.Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            JsonObject jsonObject = json.getAsJsonObject();
            String queryId = jsonObject.get("queryId").getAsString();
            Schema schema = context.deserialize(jsonObject.get("schema"), Schema.class);
            RecordJSONSerDe.RecordGsonSerialiser recordDeserialiser = new RecordJSONSerDe.RecordGsonSerialiser(schema);
            List<Record> records = new ArrayList<>();
            jsonObject.getAsJsonArray("records").iterator()
                    .forEachRemaining(j -> records.add(recordDeserialiser.deserialize(j, Record.class, context)));
            return new ResultsBatch(queryId, schema, records);
        }

        @Override
        public JsonElement serialize(ResultsBatch src, java.lang.reflect.Type typeOfSrc, JsonSerializationContext context) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("queryId", src.getQueryId());
            jsonObject.add("schema", context.serialize(src.getSchema(), Schema.class));
            JsonArray serialisedRecords = new JsonArray();
            RecordJSONSerDe.RecordGsonSerialiser recordSerialiser = new RecordJSONSerDe.RecordGsonSerialiser(src.getSchema());
            src.getRecords().stream()
                    .map(rec -> recordSerialiser.serialize(rec, Record.class, context))
                    .forEach(serialisedRecords::add);
            jsonObject.add("records", serialisedRecords);
            return jsonObject;
        }
    }
}
