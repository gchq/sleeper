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
package sleeper.splitter.core.find;

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

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionSerDe.PartitionJsonSerDe;
import sleeper.core.properties.table.TablePropertiesProvider;

import java.util.ArrayList;
import java.util.List;

/**
 * Serialises a partition splitting job to and from byte arrays and strings.
 */
public class SplitPartitionJobDefinitionSerDe {
    public static final String TABLE_ID = "tableId";
    public static final String FILE_NAMES = "filenames";
    public static final String PARTITION = "partition";

    private final Gson gson;
    private final Gson gsonPrettyPrinting;

    public SplitPartitionJobDefinitionSerDe(TablePropertiesProvider tablePropertiesProvider) {
        GsonBuilder builder = new GsonBuilder()
                .registerTypeAdapter(SplitPartitionJobDefinition.class, new SplitPartitionJobDefinitionJsonSerDe(tablePropertiesProvider))
                .serializeNulls();
        this.gson = builder.create();
        this.gsonPrettyPrinting = builder.setPrettyPrinting().create();
    }

    public String toJson(SplitPartitionJobDefinition splitPartitionJobDefinition) {
        return gson.toJson(splitPartitionJobDefinition);
    }

    public String toJson(SplitPartitionJobDefinition splitPartitionJobDefinition, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrinting.toJson(splitPartitionJobDefinition);
        }
        return toJson(splitPartitionJobDefinition);
    }

    public SplitPartitionJobDefinition fromJson(String json) {
        return gson.fromJson(json, SplitPartitionJobDefinition.class);
    }

    public static class SplitPartitionJobDefinitionJsonSerDe implements JsonSerializer<SplitPartitionJobDefinition>, JsonDeserializer<SplitPartitionJobDefinition> {
        private final TablePropertiesProvider tablePropertiesProvider;

        public SplitPartitionJobDefinitionJsonSerDe(TablePropertiesProvider tablePropertiesProvider) {
            this.tablePropertiesProvider = tablePropertiesProvider;
        }

        @Override
        public JsonElement serialize(SplitPartitionJobDefinition job, java.lang.reflect.Type typeOfSrc, JsonSerializationContext context) {
            JsonObject json = new JsonObject();
            json.addProperty(TABLE_ID, job.getTableId());

            JsonArray fileNames = new JsonArray();
            for (String fileName : job.getFileNames()) {
                fileNames.add(fileName);
            }
            json.add(FILE_NAMES, fileNames);

            PartitionJsonSerDe partitionJsonSerDe = new PartitionJsonSerDe(tablePropertiesProvider.getById(job.getTableId()).getSchema());
            JsonElement jsonPartition = partitionJsonSerDe.serialize(job.getPartition(), typeOfSrc, context);
            json.add(PARTITION, jsonPartition);
            return json;
        }

        @Override
        public SplitPartitionJobDefinition deserialize(JsonElement jsonElement, java.lang.reflect.Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            if (!jsonElement.isJsonObject()) {
                throw new JsonParseException("Expected JsonObject, got " + jsonElement);
            }
            JsonObject json = (JsonObject) jsonElement;
            String tableId = json.get(TABLE_ID).getAsString();

            JsonArray fileNamesArray = json.get(FILE_NAMES).getAsJsonArray();
            List<String> fileNames = new ArrayList<>();
            for (JsonElement element : fileNamesArray) {
                fileNames.add(element.getAsString());
            }

            PartitionJsonSerDe partitionJsonSerDe = new PartitionJsonSerDe(tablePropertiesProvider.getById(tableId).getSchema());
            JsonElement jsonPartition = json.get(PARTITION);
            Partition partition = partitionJsonSerDe.deserialize(jsonPartition, typeOfT, context);

            return new SplitPartitionJobDefinition(tableId, partition, fileNames);
        }
    }
}
