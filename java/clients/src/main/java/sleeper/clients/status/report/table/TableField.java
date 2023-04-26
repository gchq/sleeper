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

import java.util.Objects;

public class TableField implements TableFieldReference {

    private final int index;
    private final TableFieldDefinition definition;

    private TableField(Builder builder) {
        index = builder.index;
        definition = Objects.requireNonNull(builder.definition, "definition must not be null");
    }

    static Builder builder(TableWriterFactory.Builder table, int index) {
        return new Builder(table, index);
    }

    public String getHeader() {
        return definition.getHeader();
    }

    public int getIndex() {
        return index;
    }

    public TableFieldDefinition.HorizontalAlignment getHorizontalAlignment() {
        return definition.getHorizontalAlignment();
    }

    public TableFieldDefinition getDefinition() {
        return definition;
    }

    public static final class Builder {
        private final TableWriterFactory.Builder table;
        private final int index;
        private TableFieldDefinition definition;

        private Builder(TableWriterFactory.Builder table, int index) {
            this.table = table;
            this.index = index;
        }

        public Builder definition(TableFieldDefinition definition) {
            this.definition = definition;
            return this;
        }

        public TableField build() {
            return table.addField(new TableField(this));
        }

    }
}
