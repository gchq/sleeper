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
package sleeper.core.record;

import com.facebook.collections.ByteArray;

import sleeper.core.key.Key;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The fundamental data item. A {@link SleeperRow} consists of a {@link Map} from
 * {@link String} to {@link Object} where the object will be one of the supported
 * types.
 */
public class SleeperRow {
    private final Map<String, Object> values;

    public SleeperRow() {
        values = new HashMap<>();
    }

    public SleeperRow(Map<String, Object> map) {
        this();
        this.values.putAll(map);
    }

    public SleeperRow(SleeperRow record) {
        this();
        this.values.putAll(record.values);
    }

    /**
     * Gets the value of a field.
     *
     * @param  fieldName the name of the field
     * @return           the value of the field
     */
    public Object get(String fieldName) {
        return values.get(fieldName);
    }

    /**
     * Gets a key containing the values of all row keys.
     *
     * @param  schema the schema for this record
     * @return        a {@link Key} containing all row key values
     */
    public Key getRowKeys(Schema schema) {
        return Key.create(getValues(schema.getRowKeyFieldNames()));
    }

    /**
     * Removes the value of a field.
     *
     * @param fieldName the name of the field
     */
    public void remove(String fieldName) {
        this.values.remove(fieldName);
    }

    /**
     * Sets a value for a field.
     *
     * @param fieldName the name of the field
     * @param value     the value to set
     */
    public void put(String fieldName, Object value) {
        values.put(fieldName, value);
    }

    public Set<String> getKeys() {
        return Collections.unmodifiableSet(values.keySet());
    }

    /**
     * Gets the values for all provided field names.
     *
     * @param  fieldNames the names of fields
     * @return            a list of values
     */
    public List<Object> getValues(List<String> fieldNames) {
        List<Object> valuesList = new ArrayList<>();
        for (String fieldName : fieldNames) {
            valuesList.add(values.get(fieldName));
        }
        return valuesList;
    }

    @Override
    public int hashCode() {
        Map<String, Object> cloneWithWrappedByteArray = new HashMap<>();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            if (entry.getValue() instanceof byte[]) {
                cloneWithWrappedByteArray.put(entry.getKey(), ByteArray.wrap((byte[]) entry.getValue()));
            } else {
                cloneWithWrappedByteArray.put(entry.getKey(), entry.getValue());
            }
        }
        int hash = 7;
        hash = 17 * hash + Objects.hashCode(cloneWithWrappedByteArray);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final SleeperRow other = (SleeperRow) obj;

        Map<String, Object> cloneWithWrappedByteArray = new HashMap<>();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            if (entry.getValue() instanceof byte[]) {
                cloneWithWrappedByteArray.put(entry.getKey(), ByteArray.wrap((byte[]) entry.getValue()));
            } else {
                cloneWithWrappedByteArray.put(entry.getKey(), entry.getValue());
            }
        }
        Map<String, Object> otherClonedWithWrappedByteArray = new HashMap<>();
        for (Map.Entry<String, Object> entry : other.values.entrySet()) {
            if (entry.getValue() instanceof byte[]) {
                otherClonedWithWrappedByteArray.put(entry.getKey(), ByteArray.wrap((byte[]) entry.getValue()));
            } else {
                otherClonedWithWrappedByteArray.put(entry.getKey(), entry.getValue());
            }
        }
        return Objects.equals(cloneWithWrappedByteArray, otherClonedWithWrappedByteArray);
    }

    @Override
    public String toString() {
        Map<String, Object> cloneWithWrappedByteArray = new HashMap<>();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            if (entry.getValue() instanceof byte[]) {
                cloneWithWrappedByteArray.put(entry.getKey(), ByteArray.wrap((byte[]) entry.getValue()));
            } else {
                cloneWithWrappedByteArray.put(entry.getKey(), entry.getValue());
            }
        }
        return "Record{" + "values=" + cloneWithWrappedByteArray + '}';
    }

    /**
     * Returns a string representation of this record. The only fields that are shown are those present in the schema.
     *
     * @param  schema the schema to filter fields by
     * @return        a string representation of this record
     */
    public String toString(Schema schema) {
        StringBuilder stringBuilder = new StringBuilder();
        List<String> terms = new ArrayList<>();
        stringBuilder.append("Record{");
        append(schema.getRowKeyFields(), terms);
        append(schema.getSortKeyFields(), terms);
        append(schema.getValueFields(), terms);
        stringBuilder.append(String.join(", ", terms));
        stringBuilder.append("}");
        return stringBuilder.toString();
    }

    private void append(List<Field> fields, List<String> terms) {
        for (Field field : fields) {
            String term = field.getName() + "=";
            if (field.getType() instanceof ByteArrayType) {
                term += ByteArray.wrap((byte[]) values.get(field.getName()));
            } else {
                term += values.get(field.getName());
            }
            terms.add(term);
        }
    }
}
