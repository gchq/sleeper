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
package sleeper.core.row.serialiser;

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

import sleeper.core.row.Record;
import sleeper.core.row.ResultsBatch;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.schema.type.Type;

import java.util.ArrayList;
import java.util.List;

/**
 * Serialises and deserialises a batch of records to and from a JSON string.
 */
public class JSONResultsBatchSerialiser implements ResultsBatchSerialiser {
    private final Gson gson;
    private final Gson gsonPrettyPrinting;

    public JSONResultsBatchSerialiser() {
        GsonBuilder gsonBuilder = new GsonBuilder()
                .registerTypeAdapter(ResultsBatch.class, new ResultsBatchSerDe())
                .registerTypeAdapter(Type.class, new SchemaSerDe.AbstractTypeJsonSerializer())
                .registerTypeAdapter(Type.class, new SchemaSerDe.AbstractTypeJsonDeserializer())
                .serializeNulls();
        this.gson = gsonBuilder
                .create();
        this.gsonPrettyPrinting = gsonBuilder
                .setPrettyPrinting()
                .create();
    }

    @Override
    public String serialise(ResultsBatch resultsBatch) {
        return gson.toJson(resultsBatch);
    }

    @Override
    public ResultsBatch deserialise(String json) {
        return gson.fromJson(json, ResultsBatch.class);
    }

    /**
     * Serialises a batch of results to a JSON string.
     *
     * @param  resultsBatch the batch of results
     * @param  prettyPrint  whether to pretty-print the JSON string
     * @return              a serialised JSON string
     */
    public String serialise(ResultsBatch resultsBatch, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrinting.toJson(resultsBatch);
        }
        return serialise(resultsBatch);
    }

    /**
     * A GSON plugin to serialise/deserialise a batch of results.
     */
    private static class ResultsBatchSerDe implements JsonSerializer<ResultsBatch>, JsonDeserializer<ResultsBatch> {
        @Override
        public ResultsBatch deserialize(JsonElement json, java.lang.reflect.Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            JsonObject jsonObject = json.getAsJsonObject();
            String queryId = jsonObject.get("queryId").getAsString();
            Schema schema = context.deserialize(jsonObject.get("schema"), Schema.class);
            RowJsonSerDe.RowGsonSerialiser rowDeserialiser = new RowJsonSerDe.RowGsonSerialiser(schema);
            List<Record> rows = new ArrayList<>();
            jsonObject.getAsJsonArray("rows").iterator()
                    .forEachRemaining(j -> rows.add(rowDeserialiser.deserialize(j, Record.class, context)));
            return new ResultsBatch(queryId, schema, rows);
        }

        @Override
        public JsonElement serialize(ResultsBatch src, java.lang.reflect.Type typeOfSrc, JsonSerializationContext context) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("queryId", src.getQueryId());
            jsonObject.add("schema", context.serialize(src.getSchema(), Schema.class));
            JsonArray serialisedRows = new JsonArray();
            RowJsonSerDe.RowGsonSerialiser rowSerialiser = new RowJsonSerDe.RowGsonSerialiser(src.getSchema());
            src.getRecords().stream()
                    .map(rec -> rowSerialiser.serialize(rec, Record.class, context))
                    .forEach(serialisedRows::add);
            jsonObject.add("records", serialisedRows);
            return jsonObject;
        }
    }
}
