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
package sleeper.sketches;

import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantiles.ItemsUnion;

import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArray;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.Type;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class Sketches {

    private final Schema schema;
    private final Map<String, ItemsSketch> keyFieldToQuantilesSketch;

    public Sketches(Schema schema, Map<String, ItemsSketch> keyFieldToQuantilesSketch) {
        this.schema = schema;
        this.keyFieldToQuantilesSketch = keyFieldToQuantilesSketch;
    }

    public static Sketches from(Schema schema) {
        return from(schema, 1024);
    }

    public static Sketches from(Schema schema, int k) {
        Map<String, ItemsSketch> keyFieldToSketch = new HashMap<>();
        for (Field rowKeyField : schema.getRowKeyFields()) {
            keyFieldToSketch.put(rowKeyField.getName(), createSketch(rowKeyField.getType(), k));
        }
        return new Sketches(schema, keyFieldToSketch);
    }

    public static <T> ItemsSketch<T> createSketch(Type type, int k) {
        return (ItemsSketch<T>) ItemsSketch.getInstance(k, createComparator(type));
    }

    public static <T> ItemsUnion<T> createUnion(Type type) {
        return createUnion(type, 16384);
    }

    public static <T> ItemsUnion<T> createUnion(Type type, int maxK) {
        return (ItemsUnion<T>) ItemsUnion.getInstance(maxK, createComparator(type));
    }

    public static <T> Comparator<T> createComparator(Type type) {
        if (type instanceof PrimitiveType) {
            return (Comparator<T>) Comparator.naturalOrder();
        } else {
            throw new IllegalArgumentException("Unknown key type of " + type);
        }
    }

    public <T> ItemsSketch<T> getQuantilesSketch(String keyFieldName) {
        return (ItemsSketch<T>) keyFieldToQuantilesSketch.get(keyFieldName);
    }

    public Stream<FieldSketch> fieldSketches() {
        return keyFieldToQuantilesSketch.entrySet().stream()
                .map(entry -> new FieldSketch(schema.getField(entry.getKey()).orElseThrow(), entry.getValue()));
    }

    public void update(Row row) {
        for (Field rowKeyField : schema.getRowKeyFields()) {
            update(getQuantilesSketch(rowKeyField.getName()), row, rowKeyField);
        }
    }

    public static void update(ItemsSketch sketch, Row row, Field field) {
        sketch.update(convertValueForSketch(row, field));
    }

    public static Object readValueFromSketchWithWrappedBytes(Object value, Field field) {
        if (value == null) {
            return null;
        } else if (field.getType() instanceof IntType) {
            return ((Long) value).intValue();
        } else {
            return value;
        }
    }

    private static Object convertValueForSketch(Row row, Field field) {
        Object value = row.get(field.getName());
        if (value == null) {
            return null;
        } else if (field.getType() instanceof IntType) {
            return ((Integer) value).longValue();
        } else if (field.getType() instanceof ByteArrayType) {
            return ByteArray.wrap((byte[]) value);
        } else {
            return value;
        }
    }

    public static class FieldSketch {
        private final Field field;
        private final ItemsSketch sketch;

        private FieldSketch(Field field, ItemsSketch sketch) {
            this.field = field;
            this.sketch = sketch;
        }

        public Field getField() {
            return field;
        }

        public <T> ItemsSketch<T> getSketch() {
            return sketch;
        }
    }
}
