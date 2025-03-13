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
package sleeper.core.partition;

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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import sleeper.core.range.Region;
import sleeper.core.range.RegionSerDe.RegionJsonSerDe;
import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Serialises a partition to and from a JSON string.
 */
public class PartitionSerDe {
    public static final String PARTITION_ID = "partitionId";
    public static final String IS_LEAF_PARTITION = "isLeafPartition";
    public static final String PARENT_PARTITION_ID = "parentPartitionId";
    public static final String CHILD_PARTITION_IDS = "childPartitionIds";
    public static final String REGION = "region";
    public static final String DIMENSION = "dimension";

    private final Gson gson;
    private final Gson gsonPrettyPrinting;

    public PartitionSerDe(Schema schema) {
        GsonBuilder builder = new GsonBuilder()
                .registerTypeAdapter(Partition.class, new PartitionJsonSerDe(schema))
                .serializeNulls();
        this.gson = builder.create();
        this.gsonPrettyPrinting = builder.setPrettyPrinting().create();
    }

    /**
     * Serialises a partition to JSON.
     *
     * @param  partition the partition
     * @return           a JSON representation of the partition
     */
    public String toJson(Partition partition) {
        return gson.toJson(partition);
    }

    /**
     * Serialises a partition to JSON.
     *
     * @param  partition   the partition
     * @param  prettyPrint true if the JSON should be formatted for readability
     * @return             a JSON representation of the partition
     */
    public String toJson(Partition partition, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrinting.toJson(partition);
        }
        return toJson(partition);
    }

    /**
     * Deserialises a partition from JSON.
     *
     * @param  jsonSchema the JSON
     * @return            the partition represented by the JSON
     */
    public Partition fromJson(String jsonSchema) {
        return gson.fromJson(jsonSchema, Partition.class);
    }

    /**
     * A GSON plugin to serialise/deserialise a partition.
     */
    public static class PartitionJsonSerDe implements JsonSerializer<Partition>, JsonDeserializer<Partition> {
        private final RegionJsonSerDe regionJsonSerDe;

        public PartitionJsonSerDe(Schema schema) {
            this.regionJsonSerDe = new RegionJsonSerDe(schema);
        }

        @Override
        @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
        public JsonElement serialize(Partition partition, java.lang.reflect.Type typeOfSrc, JsonSerializationContext context) {
            JsonObject json = new JsonObject();
            json.addProperty(PARTITION_ID, partition.getId());
            json.addProperty(IS_LEAF_PARTITION, partition.isLeafPartition());
            json.addProperty(PARENT_PARTITION_ID, partition.getParentPartitionId());
            JsonArray childPartitionIds = new JsonArray();
            if (null != partition.getChildPartitionIds()) {
                for (String childId : partition.getChildPartitionIds()) {
                    childPartitionIds.add(childId);
                }
            }
            json.add(CHILD_PARTITION_IDS, childPartitionIds);
            json.add(REGION, regionJsonSerDe.serialize(partition.getRegion(), null, context));
            json.addProperty(DIMENSION, partition.getDimension());
            return json;
        }

        @Override
        public Partition deserialize(JsonElement jsonElement, java.lang.reflect.Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            if (!jsonElement.isJsonObject()) {
                throw new JsonParseException("Expected JsonObject, got " + jsonElement);
            }
            JsonObject json = jsonElement.getAsJsonObject();
            String partitionId = json.get(PARTITION_ID).getAsString();
            boolean isLeafPartition = json.get(IS_LEAF_PARTITION).getAsBoolean();
            String parentPartitionId = null;
            if (json.has(PARENT_PARTITION_ID)) {
                if (!json.get(PARENT_PARTITION_ID).isJsonNull()) {
                    parentPartitionId = json.get(PARENT_PARTITION_ID).getAsString();

                }
            }
            JsonArray childPartitionIdsArray = json.get(CHILD_PARTITION_IDS).getAsJsonArray();
            List<String> childPartitionIds = new ArrayList<>();
            Iterator<JsonElement> it = childPartitionIdsArray.iterator();
            while (it.hasNext()) {
                JsonElement element = it.next();
                childPartitionIds.add(element.getAsString());
            }
            Region region = regionJsonSerDe.deserialize(json.get(REGION), null, context);
            int dimension = json.get(DIMENSION).getAsInt();
            return Partition.builder()
                    .region(region)
                    .id(partitionId)
                    .leafPartition(isLeafPartition)
                    .parentPartitionId(parentPartitionId)
                    .childPartitionIds(childPartitionIds)
                    .dimension(dimension)
                    .build();
        }
    }
}
