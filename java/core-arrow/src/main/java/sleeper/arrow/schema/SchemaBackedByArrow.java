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

package sleeper.arrow.schema;


import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static sleeper.arrow.schema.ArrowSchemaConverter.convertSleeperSchemaToArrowSchema;

public class SchemaBackedByArrow {
    private final org.apache.arrow.vector.types.pojo.Schema arrowSchema;
    private final List<String> rowKeyFieldNames;
    private final List<String> sortKeyFieldNames;
    private final List<String> valueFieldNames;

    private SchemaBackedByArrow(Builder builder) {
        arrowSchema = Objects.requireNonNull(builder.arrowSchema, "arrowSchema must not be null");
        rowKeyFieldNames = Objects.requireNonNull(builder.rowKeyFieldNames, "rowKeyFieldNames must not be null");
        sortKeyFieldNames = Objects.requireNonNull(builder.sortKeyFieldNames, "sortKeyFieldNames must not be null");
        valueFieldNames = arrowSchema.getFields().stream()
                .map(org.apache.arrow.vector.types.pojo.Field::getName)
                .filter(field -> !rowKeyFieldNames.contains(field) && !sortKeyFieldNames.contains(field))
                .collect(Collectors.toList());
        if (valueFieldNames.isEmpty()) {
            throw new IllegalArgumentException("No value fields defined");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static SchemaBackedByArrow fromSleeperSchema(Schema sleeperSchema) {
        return builder()
                .arrowSchema(convertSleeperSchemaToArrowSchema(sleeperSchema))
                .sleeperSchema(sleeperSchema)
                .build();
    }

    public static SchemaBackedByArrow fromArrowSchema(org.apache.arrow.vector.types.pojo.Schema arrowSchema,
                                                      List<String> rowKeyFieldNames, List<String> sortKeyFieldNames) {
        return builder()
                .arrowSchema(arrowSchema)
                .rowKeyFieldNames(rowKeyFieldNames)
                .sortKeyFieldNames(sortKeyFieldNames)
                .build();
    }

    public org.apache.arrow.vector.types.pojo.Schema getArrowSchema() {
        return arrowSchema;
    }

    public List<Field> getRowKeyFields() {
        return getFields(rowKeyFieldNames);
    }

    public List<Field> getSortKeyFields() {
        return getFields(sortKeyFieldNames);
    }

    public List<org.apache.arrow.vector.types.pojo.Field> getValueFields() {
        return valueFieldNames.stream()
                .map(arrowSchema::findField)
                .collect(Collectors.toList());
    }

    private List<Field> getFields(List<String> fieldNames) {
        return fieldNames.stream()
                .map(arrowSchema::findField)
                .map(ArrowFieldConverter::convertArrowFieldToSleeperField)
                .collect(Collectors.toList());
    }

    public static final class Builder {
        private org.apache.arrow.vector.types.pojo.Schema arrowSchema;
        private List<String> rowKeyFieldNames;
        private List<String> sortKeyFieldNames;

        private Builder() {
        }

        public Builder arrowSchema(org.apache.arrow.vector.types.pojo.Schema arrowSchema) {
            this.arrowSchema = arrowSchema;
            return this;
        }

        public Builder sleeperSchema(Schema schema) {
            return this.rowKeyFieldNames(schema.getRowKeyFieldNames())
                    .sortKeyFieldNames(schema.getSortKeyFieldNames());
        }

        public Builder rowKeyFieldNames(List<String> rowKeyFieldNames) {
            this.rowKeyFieldNames = rowKeyFieldNames;
            return this;
        }

        public Builder sortKeyFieldNames(List<String> sortKeyFieldNames) {
            this.sortKeyFieldNames = sortKeyFieldNames;
            return this;
        }

        public SchemaBackedByArrow build() {
            return new SchemaBackedByArrow(this);
        }
    }
}
