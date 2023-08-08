/*
 * Copyright 2022-2023 Crown Copyright
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Schema describes the fields found in a particular table in a Sleeper instance.
 */
public class Schema {
    private final List<Field> rowKeyFields;
    private final List<Field> sortKeyFields;
    private final List<Field> valueFields;

    private Schema(Builder builder) {
        rowKeyFields = validateRowKeys(builder.rowKeyFields);
        sortKeyFields = validateSortKeys(builder.sortKeyFields);
        valueFields = validateValueKeys(builder.valueFields);
        validateNoDuplicates(streamAllFields(rowKeyFields, sortKeyFields, valueFields));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Schema load(Path schemaPath) throws IOException {
        return loadFromString(Files.readString(schemaPath));
    }

    public static Schema loadFromString(String schemaJson) {
        return new SchemaSerDe().fromJson(schemaJson);
    }

    public List<Field> getRowKeyFields() {
        return rowKeyFields;
    }

    public List<Field> getSortKeyFields() {
        return sortKeyFields;
    }

    public List<Field> getValueFields() {
        return valueFields;
    }

    public List<PrimitiveType> getRowKeyTypes() {
        return getMappedFields(rowKeyFields, f -> (PrimitiveType) f.getType());
    }

    public List<PrimitiveType> getSortKeyTypes() {
        return getMappedFields(sortKeyFields, f -> (PrimitiveType) f.getType());
    }

    public List<String> getRowKeyFieldNames() {
        return getMappedFields(rowKeyFields, Field::getName);
    }

    public List<String> getSortKeyFieldNames() {
        return getMappedFields(sortKeyFields, Field::getName);
    }

    public List<String> getValueFieldNames() {
        return getMappedFields(valueFields, Field::getName);
    }

    public List<String> getAllFieldNames() {
        return getMappedFields(streamAllFields(), Field::getName);
    }

    private <T> List<T> getMappedFields(List<Field> fields, Function<Field, T> mapping) {
        return getMappedFields(fields.stream(), mapping);
    }

    private <T> List<T> getMappedFields(Stream<Field> fields, Function<Field, T> mapping) {
        return fields
                .map(mapping)
                .collect(Collectors.toUnmodifiableList());
    }

    public List<Field> getAllFields() {
        return streamAllFields().collect(Collectors.toUnmodifiableList());
    }

    public Stream<Field> streamAllFields() {
        return streamAllFields(rowKeyFields, sortKeyFields, valueFields);
    }

    public Optional<Field> getField(String fieldName) {
        return streamAllFields()
                .filter(f -> f.getName().equals(fieldName))
                .findFirst();
    }


    public Field getField(Integer index) {
        return streamAllFields().collect(Collectors.toList()).get(index);
    }

    public void save(Path path) throws IOException {
        Files.writeString(path, new SchemaSerDe().toJson(this));
    }

    @Override
    public String toString() {
        return "Schema{" + "rowKeyFields=" + rowKeyFields + ", sortKeyFields=" + sortKeyFields + ", valueFields=" + valueFields + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
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
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("Must have at least one row key field");
        }
        if (fields.stream().anyMatch(field -> !(field.getType() instanceof PrimitiveType))) {
            throw new IllegalArgumentException("Row key fields must have a primitive type");
        }
        return makeImmutable(fields);
    }

    private static List<Field> validateSortKeys(List<Field> fields) {
        if (fields == null) {
            return Collections.emptyList();
        }
        if (fields.stream().anyMatch(field -> !(field.getType() instanceof PrimitiveType))) {
            throw new IllegalArgumentException("Sort key fields must have a primitive type");
        }
        return makeImmutable(fields);
    }

    private static List<Field> validateValueKeys(List<Field> fields) {
        if (fields == null) {
            return Collections.emptyList();
        }
        return makeImmutable(fields);
    }

    private static List<Field> makeImmutable(List<Field> fields) {
        return Collections.unmodifiableList(new ArrayList<>(fields));
    }

    private static Stream<Field> streamAllFields(
            List<Field> rowKeyFields, List<Field> sortKeyFields, List<Field> valueFields) {

        return Stream.of(rowKeyFields, sortKeyFields, valueFields)
                .flatMap(List::stream);
    }

    private static void validateNoDuplicates(Stream<Field> fields) {
        Set<String> foundNames = new HashSet<>();
        Set<String> duplicates = new TreeSet<>();
        fields.forEach(field -> {
            boolean isNew = foundNames.add(field.getName());
            if (!isNew) {
                duplicates.add(field.getName());
            }
        });
        if (!duplicates.isEmpty()) {
            throw new IllegalArgumentException("Found duplicate field names: " + duplicates);
        }
    }
}
