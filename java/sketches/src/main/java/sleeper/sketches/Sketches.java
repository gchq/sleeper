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

import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class Sketches {
    private final Map<String, ItemsSketch> keyFieldToQuantilesSketch;

    public Sketches(Map<String, ItemsSketch> keyFieldToQuantilesSketch) {
        this.keyFieldToQuantilesSketch = keyFieldToQuantilesSketch;
    }

    public static Sketches from(Schema schema) {
        Map<String, ItemsSketch> keyFieldToSketch = new HashMap<>();
        for (Field rowKeyField : schema.getRowKeyFields()) {
            keyFieldToSketch.put(rowKeyField.getName(), createSketch(rowKeyField));
        }
        return new Sketches(keyFieldToSketch);
    }

    private static ItemsSketch<?> createSketch(Field field) {
        if (field.getType() instanceof IntType || field.getType() instanceof LongType) {
            Comparator<Number> comparator = (Comparator<Number>) Comparator.naturalOrder();
            return ItemsSketch.getInstance(Number.class, 1024, comparator);
        } else if (field.getType() instanceof StringType) {
            return ItemsSketch.getInstance(String.class, 1024, Comparator.naturalOrder());
        } else if (field.getType() instanceof ByteArrayType) {
            return ItemsSketch.getInstance(ByteArray.class, 1024, Comparator.naturalOrder());
        } else {
            throw new IllegalArgumentException("Unknown key type of " + field.getType());
        }
    }

    public Map<String, ItemsSketch> getQuantilesSketches() {
        return keyFieldToQuantilesSketch;
    }

    public <T> ItemsSketch<T> getQuantilesSketch(String keyFieldName) {
        return (ItemsSketch<T>) keyFieldToQuantilesSketch.get(keyFieldName);
    }

    public void update(Schema schema, Record record) {
        for (Field rowKeyField : schema.getRowKeyFields()) {
            if (rowKeyField.getType() instanceof ByteArrayType) {
                byte[] value = (byte[]) record.get(rowKeyField.getName());
                getQuantilesSketch(rowKeyField.getName()).update(ByteArray.wrap(value));
            } else {
                Object value = record.get(rowKeyField.getName());
                getQuantilesSketch(rowKeyField.getName()).update(value);
            }
        }
    }
}
