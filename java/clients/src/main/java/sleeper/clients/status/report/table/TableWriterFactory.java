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
package sleeper.clients.status.report.table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TableWriterFactory {

    private final TableStructure structure;
    private final List<TableField> fields;

    private TableWriterFactory(Builder builder) {
        structure = builder.structure;
        fields = Collections.unmodifiableList(new ArrayList<>(builder.fields));
    }

    public static Builder builder() {
        return new Builder();
    }

    public TableWriter.Builder tableBuilder() {
        return new TableWriter.Builder(structure, fields);
    }

    public static final class Builder {
        private final List<TableField> fields = new ArrayList<>();
        private TableStructure structure = TableStructure.DEFAULT;

        private Builder() {
        }

        public TableField addField(String header) {
            return addField(TableFieldDefinition.field(header));
        }

        public TableField addNumericField(String header) {
            return addField(TableFieldDefinition.numeric(header));
        }

        public TableField addField(TableFieldDefinition definition) {
            return TableField.builder(this, fields.size())
                    .definition(definition)
                    .build();
        }

        public Builder addFields(TableFieldDefinition... definitions) {
            for (TableFieldDefinition definition : definitions) {
                addField(definition);
            }
            return this;
        }

        TableField addField(TableField field) {
            fields.add(field);
            return field;
        }

        public Builder structure(TableStructure structure) {
            this.structure = structure;
            return this;
        }

        public TableWriterFactory build() {
            return new TableWriterFactory(this);
        }
    }
}
