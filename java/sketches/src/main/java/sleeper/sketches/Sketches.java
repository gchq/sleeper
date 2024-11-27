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
package sleeper.sketches;

import com.facebook.collections.ByteArray;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantiles.ItemsUnion;

import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.Type;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class Sketches {

    private final Schema schema;
    private final Map<String, ItemsSketch> keyFieldToQuantilesSketch;

    public Sketches(Schema schema, Map<String, ItemsSketch> keyFieldToQuantilesSketch) {
        this.schema = schema;
        this.keyFieldToQuantilesSketch = keyFieldToQuantilesSketch;
    }

    public static Sketches from(Schema schema) {
        Map<String, ItemsSketch> keyFieldToSketch = new HashMap<>();
        for (Field rowKeyField : schema.getRowKeyFields()) {
            keyFieldToSketch.put(rowKeyField.getName(), createSketch(rowKeyField.getType(), 1024));
        }
        return new Sketches(schema, keyFieldToSketch);
    }

    public static <T> ItemsSketch<T> createSketch(Type type, int k) {
        if (type instanceof PrimitiveType) {
            return (ItemsSketch<T>) ItemsSketch.getInstance(k, Comparator.naturalOrder());
        } else {
            throw new IllegalArgumentException("Unknown key type of " + type);
        }
    }

    public static <T> ItemsUnion<T> createUnion(Type type, int maxK) {
        if (type instanceof PrimitiveType) {
            return (ItemsUnion<T>) ItemsUnion.getInstance(maxK, Comparator.naturalOrder());
        } else {
            throw new IllegalArgumentException("Unknown key type of " + type);
        }
    }

    public static <T> Comparator<T> createComparator(Type type) {
        if (type instanceof PrimitiveType) {
            return (Comparator<T>) Comparator.naturalOrder();
        } else {
            throw new IllegalArgumentException("Unknown key type of " + type);
        }
    }

    public Map<String, ItemsSketch> getQuantilesSketches() {
        return keyFieldToQuantilesSketch;
    }

    public <T> ItemsSketch<T> getQuantilesSketch(String keyFieldName) {
        return (ItemsSketch<T>) keyFieldToQuantilesSketch.get(keyFieldName);
    }

    public void update(Record record) {
        for (Field rowKeyField : schema.getRowKeyFields()) {
            update(getQuantilesSketch(rowKeyField.getName()), record, rowKeyField);
        }
    }

    public static void update(ItemsSketch sketch, Record record, Field field) {
        sketch.update(convertValue(record, field));
    }

    public static Object readValueFromSketch(Object value, Field field) {
        if (value == null) {
            return null;
        } else if (field.getType() instanceof ByteArrayType) {
            return ((ByteArray) value).getArray();
        } else {
            return value;
        }
    }

    private static Object convertValue(Record record, Field field) {
        Object value = record.get(field.getName());
        if (value == null) {
            return null;
        } else if (field.getType() instanceof ByteArrayType) {
            return ByteArray.wrap((byte[]) value);
        } else {
            return record.get(field.getName());
        }
    }
}
