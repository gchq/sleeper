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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import sleeper.core.schema.Field;
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
 * Serialises and deserialises values of Sleeper fields to and from JSON.
 */
public class FieldValueJsonSerDe {

    private FieldValueJsonSerDe() {
    }

    /**
     * Converts a value to a JsonElement.
     *
     * @param  field      the field
     * @param  fieldValue the value
     * @return            the element
     */
    @SuppressWarnings("unchecked")
    public static JsonElement toJsonElement(Field field, Object fieldValue) {
        if (fieldValue == null) {
            return JsonNull.INSTANCE;
        } else if (field.getType() instanceof IntType) {
            return new JsonPrimitive((Integer) fieldValue);
        } else if (field.getType() instanceof LongType) {
            return new JsonPrimitive((Long) fieldValue);
        } else if (field.getType() instanceof StringType) {
            return new JsonPrimitive((String) fieldValue);
        } else if (field.getType() instanceof ByteArrayType) {
            byte[] bytes = (byte[]) fieldValue;
            String base64encodedBytes = Base64.getEncoder().encodeToString(bytes);
            return new JsonPrimitive(base64encodedBytes);
        } else if (field.getType() instanceof ListType) {
            return listToJsonElement(field, (List<Object>) fieldValue);
        } else if (field.getType() instanceof MapType) {
            return mapToJsonElement(field, (Map<Object, Object>) fieldValue);
        } else {
            throw new IllegalArgumentException("Unknown type " + field.getType());
        }
    }

    /**
     * Reads a JsonElement as a value of a field.
     *
     * @param  field   the field
     * @param  element the element
     * @return         the field value
     */
    public static Object fromJsonElement(Field field, JsonElement element) {
        if (element == null || element.isJsonNull()) {
            return null;
        }
        if (field.getType() instanceof IntType) {
            return element.getAsInt();
        } else if (field.getType() instanceof LongType) {
            return element.getAsLong();
        } else if (field.getType() instanceof StringType) {
            return element.getAsString();
        } else if (field.getType() instanceof ByteArrayType) {
            return Base64.getDecoder().decode(element.getAsString());
        } else if (field.getType() instanceof ListType) {
            return getListFromJsonElement(field, element);
        } else if (field.getType() instanceof MapType) {
            return getMapFromJsonElement(field, element);
        } else {
            throw new IllegalArgumentException("Unknown type " + field.getType());
        }
    }

    private static JsonElement listToJsonElement(Field field, List<Object> fieldValue) {
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
        return array;
    }

    private static JsonElement mapToJsonElement(Field field, Map<Object, Object> fieldValue) {
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
        return map;
    }

    private static List<Object> getListFromJsonElement(Field field, JsonElement element) {
        PrimitiveType elementType = ((ListType) field.getType()).getElementType();
        JsonArray array = element.getAsJsonArray();
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
        return list;
    }

    private static Map<Object, Object> getMapFromJsonElement(Field field, JsonElement element) {
        PrimitiveType keyType = ((MapType) field.getType()).getKeyType();
        PrimitiveType valueType = ((MapType) field.getType()).getValueType();

        JsonObject map = element.getAsJsonObject();
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
        return deserialisedMap;
    }

}
