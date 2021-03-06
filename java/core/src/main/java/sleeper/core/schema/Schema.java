/*
 * Copyright 2022 Crown Copyright
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
package sleeper.core.schema;

import sleeper.core.schema.type.PrimitiveType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Describes the fields for a particular instance of a Sleeper database.
 */
public class Schema {
    private final List<Field> rowKeyFields;
    private final List<Field> sortKeyFields;
    private final List<Field> valueFields;
    
    public Schema() {
        this.rowKeyFields = new ArrayList<>();
        this.sortKeyFields = new ArrayList<>();
        this.valueFields = new ArrayList<>();
    }

    // TODO Should check that names are unique
    public void setRowKeyFields(List<Field> rowKeyFields) {
        for (Field field : rowKeyFields) {
            if (!(field.getType() instanceof PrimitiveType)) {
                throw new IllegalArgumentException("Row key fields must have a primitive type");
            }
        }
        this.rowKeyFields.clear();
        this.rowKeyFields.addAll(rowKeyFields);
    }
    
    public void setRowKeyFields(Field... rowKeyFields) {
        for (Field field : rowKeyFields) {
            if (!(field.getType() instanceof PrimitiveType)) {
                throw new IllegalArgumentException("Row key fields must have a primitive type");
            }
        }
        this.rowKeyFields.clear();
        for (Field field : rowKeyFields) {
            this.rowKeyFields.add(field);
        }
    }
    
    public List<Field> getRowKeyFields() {
        return rowKeyFields;
    }
    
    public List<PrimitiveType> getRowKeyTypes() {
        return rowKeyFields.stream().map(Field::getType).map(t -> (PrimitiveType) t).collect(Collectors.toList());
    }

    public void setSortKeyFields(List<Field> sortKeyFields) {
        for (Field field : sortKeyFields) {
            if (!(field.getType() instanceof PrimitiveType)) {
                throw new IllegalArgumentException("Sort key fields must have a primitive type");
            }
        }
        this.sortKeyFields.clear();
        this.sortKeyFields.addAll(sortKeyFields);
    }
    
    public void setSortKeyFields(Field... sortKeyFields) {
        for (Field field : sortKeyFields) {
            if (!(field.getType() instanceof PrimitiveType)) {
                throw new IllegalArgumentException("Sort key fields must have a primitive type");
            }
        }
        this.sortKeyFields.clear();
        for (Field field : sortKeyFields) {
            this.sortKeyFields.add(field);
        }
    }
    
    public List<Field> getSortKeyFields() {
        return sortKeyFields;
    }

    public List<PrimitiveType> getSortKeyTypes() {
        return sortKeyFields.stream().map(Field::getType).map(t -> (PrimitiveType) t).collect(Collectors.toList());
    }
    
    public void setValueFields(List<Field> valueFields) {
        this.valueFields.clear();
        this.valueFields.addAll(valueFields);
    }
    
    public void setValueFields(Field... valueFields) {
        this.valueFields.clear();
        for (Field field : valueFields) {
            this.valueFields.add(field);
        }
    }
    
    public List<Field> getValueFields() {
        return valueFields;
    }

    public List<String> getRowKeyFieldNames() {
        return rowKeyFields.stream().map(Field::getName).collect(Collectors.toList());
    }
    
    public List<String> getSortKeyFieldNames() {
        return sortKeyFields.stream().map(Field::getName).collect(Collectors.toList());
    }
    
    public List<String> getValueFieldNames() {
        return valueFields.stream().map(Field::getName).collect(Collectors.toList());
    }
    
    public List<String> getAllFieldNames() {
        List<String> allFieldNames = new ArrayList<>();
        allFieldNames.addAll(getRowKeyFieldNames());
        allFieldNames.addAll(getSortKeyFieldNames());
        allFieldNames.addAll(getValueFieldNames());
        return allFieldNames;
    }
    
    public List<Field> getAllFields() {
        List<Field> allFields = new ArrayList<>();
        allFields.addAll(rowKeyFields);
        allFields.addAll(sortKeyFields);
        allFields.addAll(valueFields);
        return allFields;
    }

    public Optional<Field> getField(String fieldName) {
        return getAllFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .findFirst();
    }
    
    @Override
    public String toString() {
        return "Schema{" + "rowKeyFields=" + rowKeyFields + ", sortKeyFields=" + sortKeyFields + ", valueFields=" + valueFields + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Schema schema = (Schema) o;

        return Objects.equals(rowKeyFields, schema.rowKeyFields) &&
                Objects.equals(sortKeyFields, schema.sortKeyFields) &&
                Objects.equals(valueFields, schema.valueFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowKeyFields, sortKeyFields, valueFields);
    }
}
