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
package sleeper.core.record.serialiser;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import sleeper.core.record.SleeperRow;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serialises and deserialises a record to and from a JSON string.
 */
public class RecordJSONSerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrinting;

    public RecordJSONSerDe(Schema schema) {
        this.gson = new GsonBuilder()
                .registerTypeAdapter(SleeperRow.class, new RecordGsonSerialiser(schema))
                .serializeNulls()
                .create();
        this.gsonPrettyPrinting = new GsonBuilder()
                .setPrettyPrinting()
                .registerTypeAdapter(SleeperRow.class, new RecordGsonSerialiser(schema))
                .serializeNulls()
                .create();
    }

    /**
     * Serialises a record to a JSON string.
     *
     * @param  record the record
     * @return        a JSON string
     */
    public String toJson(SleeperRow record) {
        return gson.toJson(record);
    }

    /**
     * Serialises a record to a JSON string.
     *
     * @param  record      the record
     * @param  prettyPrint whether to pretty-print the JSON string
     * @return             a JSON string
     */
    public String toJson(SleeperRow record, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrinting.toJson(record);
        }
        return toJson(record);
    }

    /**
     * Deserialises a JSON string to a record.
     *
     * @param  jsonSchema the JSON string
     * @return            a record
     */
    public SleeperRow fromJson(String jsonSchema) {
        return gson.fromJson(jsonSchema, SleeperRow.class);
    }

    /**
     * A GSON plugin to serialise/deserialise a record.
     */
    public static class RecordGsonSerialiser implements JsonSerializer<SleeperRow>, JsonDeserializer<SleeperRow> {
        private final Schema schema;

        public RecordGsonSerialiser(Schema schema) {
            this.schema = schema;
        }

        @Override
        public JsonElement serialize(SleeperRow record, java.lang.reflect.Type typeOfSrc, JsonSerializationContext context) {
            JsonObject json = new JsonObject();
            for (Field field : schema.getAllFields()) {
                addFieldToJsonObject(field, record.get(field.getName()), json);
            }
            return json;
        }

        @Override
        public SleeperRow deserialize(JsonElement jsonElement, java.lang.reflect.Type typeOfSrc, JsonDeserializationContext context) throws JsonParseException {
            if (!jsonElement.isJsonObject()) {
                throw new JsonParseException("Expected JsonObject, got " + jsonElement);
            }
            SleeperRow record = new SleeperRow();
            for (Field field : schema.getAllFields()) {
                getFieldFromJsonObject(field, jsonElement.getAsJsonObject(), record);
            }
            return record;
        }
    }

    private static void addFieldToJsonObject(Field field, Object fieldValue, JsonObject json) {
        if (field.getType() instanceof IntType) {
            json.addProperty(field.getName(), (Integer) fieldValue);
        } else if (field.getType() instanceof LongType) {
            json.addProperty(field.getName(), (Long) fieldValue);
        } else if (field.getType() instanceof StringType) {
            json.addProperty(field.getName(), (String) fieldValue);
        } else if (field.getType() instanceof ByteArrayType) {
            byte[] bytes = (byte[]) fieldValue;
            if (null != bytes) {
                String base64encodedBytes = Base64.getEncoder().encodeToString(bytes);
                json.addProperty(field.getName(), base64encodedBytes);
            } else {
                json.addProperty(field.getName(), (String) null);
            }
        } else if (field.getType() instanceof ListType) {
            addListToJsonObject(field, (List<Object>) fieldValue, json);
        } else if (field.getType() instanceof MapType) {
            addMapToJsonObject(field, (Map<Object, Object>) fieldValue, json);
        } else {
            throw new IllegalArgumentException("Unknown type " + field.getType());
        }
    }

    private static void addListToJsonObject(Field field, List<Object> fieldValue, JsonObject json) {
        PrimitiveType elementType = ((ListType) field.getType()).getElementType();
        JsonArray array = new JsonArray();
        if (elementType instanceof IntType) {
            for (Object o : fieldValue) {
                array.add((Integer) o);
            }
        } else if (elementType instanceof LongType) {
            for (Object o : fieldValue) {
                array.add((Long) o);
            }
        } else if (elementType instanceof StringType) {
            for (Object o : fieldValue) {
                array.add((String) o);
            }
        } else if (elementType instanceof ByteArrayType) {
            for (Object o : fieldValue) {
                if (null != o) {
                    array.add(Base64.getEncoder().encodeToString((byte[]) o));
                } else {
                    array.add(JsonNull.INSTANCE);
                }
            }
        } else {
            throw new IllegalArgumentException("Unknown type " + field.getType());
        }
        json.add(field.getName(), array);
    }

    private static void addMapToJsonObject(Field field, Map<Object, Object> fieldValue, JsonObject json) {
        PrimitiveType keyType = ((MapType) field.getType()).getKeyType();
        PrimitiveType valueType = ((MapType) field.getType()).getValueType();

        JsonObject map = new JsonObject();
        for (Map.Entry<Object, Object> entry : fieldValue.entrySet()) {
            String key;
            if (keyType instanceof IntType || keyType instanceof LongType || keyType instanceof StringType) {
                key = entry.getKey().toString();
            } else if (keyType instanceof ByteArrayType) {
                byte[] bytes = (byte[]) entry.getKey();
                key = Base64.getEncoder().encodeToString(bytes);
            } else {
                throw new IllegalArgumentException("Unknown type " + field.getType());
            }
            if (valueType instanceof IntType || valueType instanceof LongType) {
                map.addProperty(key, (Number) entry.getValue());
            } else if (valueType instanceof StringType) {
                map.addProperty(key, (String) entry.getValue());
            } else {
                throw new IllegalArgumentException("Unknown type " + field.getType());
            }
        }
        json.add(field.getName(), map);
    }

    private static void getFieldFromJsonObject(Field field, JsonObject json, SleeperRow record) {
        if (field.getType() instanceof IntType) {
            record.put(field.getName(), json.get(field.getName()).getAsInt());
        } else if (field.getType() instanceof LongType) {
            record.put(field.getName(), json.get(field.getName()).getAsLong());
        } else if (field.getType() instanceof StringType) {
            record.put(field.getName(), json.get(field.getName()).getAsString());
        } else if (field.getType() instanceof ByteArrayType) {
            String encodedByteArray = json.get(field.getName()).getAsString();
            record.put(field.getName(), Base64.getDecoder().decode(encodedByteArray));
        } else if (field.getType() instanceof ListType) {
            getListFromJsonObject(field, json, record);
        } else if (field.getType() instanceof MapType) {
            getMapFromJsonObject(field, json, record);
        } else {
            throw new IllegalArgumentException("Unknown type " + field.getType());
        }
    }

    private static void getListFromJsonObject(Field field, JsonObject json, SleeperRow record) {
        PrimitiveType elementType = ((ListType) field.getType()).getElementType();
        JsonArray array = json.get(field.getName()).getAsJsonArray();
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
            if (elementType instanceof IntType) {
                list.add(array.get(i).getAsInt());
            } else if (elementType instanceof LongType) {
                list.add(array.get(i).getAsLong());
            } else if (elementType instanceof StringType) {
                list.add(array.get(i).getAsString());
            } else if (elementType instanceof ByteArrayType) {
                String encodedByteArray = array.get(i).getAsString();
                list.add(Base64.getDecoder().decode(encodedByteArray));
            } else {
                throw new IllegalArgumentException("Unknown type " + elementType);
            }
        }
        record.put(field.getName(), list);
    }

    private static void getMapFromJsonObject(Field field, JsonObject json, SleeperRow record) {
        PrimitiveType keyType = ((MapType) field.getType()).getKeyType();
        PrimitiveType valueType = ((MapType) field.getType()).getValueType();

        JsonObject map = json.getAsJsonObject(field.getName());
        Map<Object, Object> deserialisedMap = new HashMap<>();
        for (Map.Entry<String, JsonElement> entry : map.entrySet()) {
            String keyString = entry.getKey();
            Object key;
            if (keyType instanceof IntType) {
                key = Integer.parseInt(keyString);
            } else if (keyType instanceof LongType) {
                key = Long.parseLong(keyString);
            } else if (keyType instanceof StringType) {
                key = keyString;
            } else if (keyType instanceof ByteArrayType) {
                key = Base64.getDecoder().decode(keyString);
            } else {
                throw new IllegalArgumentException("Unknown type " + keyType);
            }
            JsonElement valueElement = entry.getValue();
            Object value;
            if (valueType instanceof IntType) {
                value = valueElement.getAsInt();
            } else if (valueType instanceof LongType) {
                value = valueElement.getAsLong();
            } else if (valueType instanceof StringType) {
                value = valueElement.getAsString();
            } else if (valueType instanceof ByteArrayType) {
                String encodedByteArray = valueElement.getAsString();
                value = Base64.getDecoder().decode(encodedByteArray);
            } else {
                throw new IllegalArgumentException("Unknown type " + keyType);
            }
            deserialisedMap.put(key, value);
        }
        record.put(field.getName(), deserialisedMap);
    }
}
