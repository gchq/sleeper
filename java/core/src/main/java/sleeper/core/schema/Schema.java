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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A Schema describes the fields found in a particular table in a Sleeper instance.
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

    private Schema(Builder builder) {
        rowKeyFields = new ArrayList<>(validateRowKeys(builder.rowKeyFields));
        sortKeyFields = new ArrayList<>(validateSortKeys(builder.sortKeyFields));
        valueFields = new ArrayList<>(builder.valueFields);
    }

    public static Builder builder() {
        return new Builder();
    }

    // TODO Should check that names are unique
    public void setRowKeyFields(List<Field> rowKeyFields) {
        validateRowKeys(rowKeyFields);
        this.rowKeyFields.clear();
        this.rowKeyFields.addAll(rowKeyFields);
    }

    public void setRowKeyFields(Field... rowKeyFields) {
        setRowKeyFields(Arrays.asList(rowKeyFields));
    }

    public List<Field> getRowKeyFields() {
        return rowKeyFields;
    }

    public List<PrimitiveType> getRowKeyTypes() {
        return rowKeyFields.stream().map(Field::getType).map(t -> (PrimitiveType) t).collect(Collectors.toList());
    }

    public void setSortKeyFields(List<Field> sortKeyFields) {
        validateSortKeys(sortKeyFields);
        this.sortKeyFields.clear();
        this.sortKeyFields.addAll(sortKeyFields);
    }

    public void setSortKeyFields(Field... sortKeyFields) {
        setSortKeyFields(Arrays.asList(sortKeyFields));
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
        setValueFields(Arrays.asList(valueFields));
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

    public static final class Builder {
        private List<Field> rowKeyFields;
        private List<Field> sortKeyFields;
        private List<Field> valueFields;

        private Builder() {
        }

        public Builder rowKeyFields(List<Field> rowKeyFields) {
            this.rowKeyFields = rowKeyFields;
            return this;
        }

        public Builder rowKeyFields(Field... rowKeyFields) {
            return rowKeyFields(Arrays.asList(rowKeyFields));
        }

        public Builder sortKeyFields(List<Field> sortKeyFields) {
            this.sortKeyFields = sortKeyFields;
            return this;
        }

        public Builder sortKeyFields(Field... sortKeyFields) {
            return sortKeyFields(Arrays.asList(sortKeyFields));
        }

        public Builder valueFields(List<Field> valueFields) {
            this.valueFields = valueFields;
            return this;
        }

        public Builder valueFields(Field... valueFields) {
            return valueFields(Arrays.asList(valueFields));
        }

        public Schema build() {
            return new Schema(this);
        }
    }

    private static List<Field> validateRowKeys(List<Field> fields) {
        if (fields.stream().anyMatch(field -> !(field.getType() instanceof PrimitiveType))) {
            throw new IllegalArgumentException("Row key fields must have a primitive type");
        }
        return fields;
    }

    private static List<Field> validateSortKeys(List<Field> fields) {
        if (fields.stream().anyMatch(field -> !(field.getType() instanceof PrimitiveType))) {
            throw new IllegalArgumentException("Sort key fields must have a primitive type");
        }
        return fields;
    }
}
