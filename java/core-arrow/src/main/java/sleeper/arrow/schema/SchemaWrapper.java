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


import sleeper.core.schema.Schema;

import java.util.List;
import java.util.Objects;

import static sleeper.arrow.schema.SchemaConverter.convertArrowSchemaToSleeperSchema;
import static sleeper.arrow.schema.SchemaConverter.convertSleeperSchemaToArrowSchema;

public class SchemaWrapper {
    private final org.apache.arrow.vector.types.pojo.Schema arrowSchema;
    private final List<String> rowKeyFieldNames;
    private final List<String> sortKeyFieldNames;
    private final List<String> valueFieldNames;

    private SchemaWrapper(Builder builder) {
        arrowSchema = Objects.requireNonNull(builder.arrowSchema, "arrowSchema must not be null");
        rowKeyFieldNames = Objects.requireNonNull(builder.rowKeyFieldNames, "rowKeyFieldNames must not be null");
        sortKeyFieldNames = Objects.requireNonNull(builder.sortKeyFieldNames, "sortKeyFieldNames must not be null");
        valueFieldNames = Objects.requireNonNull(builder.valueFieldNames, "valueFieldNames must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static SchemaWrapper fromSleeperSchema(Schema sleeperSchema) {
        return builder()
                .arrowSchema(convertSleeperSchemaToArrowSchema(sleeperSchema))
                .sleeperSchema(sleeperSchema)
                .build();
    }

    public static SchemaWrapper fromArrowSchema(org.apache.arrow.vector.types.pojo.Schema arrowSchema,
                                                List<String> rowKeyFieldNames, List<String> sortKeyFieldNames, List<String> valueFieldNames) {
        return builder()
                .arrowSchema(arrowSchema)
                .rowKeyFieldNames(rowKeyFieldNames)
                .sortKeyFieldNames(sortKeyFieldNames)
                .valueFieldNames(valueFieldNames)
                .build();
    }

    public org.apache.arrow.vector.types.pojo.Schema getArrowSchema() {
        return arrowSchema;
    }

    public Schema getSleeperSchema() {
        return convertArrowSchemaToSleeperSchema(arrowSchema, rowKeyFieldNames, sortKeyFieldNames, valueFieldNames);
    }

    public static final class Builder {
        private org.apache.arrow.vector.types.pojo.Schema arrowSchema;
        private List<String> rowKeyFieldNames;
        private List<String> sortKeyFieldNames;
        private List<String> valueFieldNames;

        private Builder() {
        }

        public Builder arrowSchema(org.apache.arrow.vector.types.pojo.Schema arrowSchema) {
            this.arrowSchema = arrowSchema;
            return this;
        }

        public Builder sleeperSchema(Schema schema) {
            return this.rowKeyFieldNames(schema.getRowKeyFieldNames())
                    .sortKeyFieldNames(schema.getSortKeyFieldNames())
                    .valueFieldNames(schema.getValueFieldNames());
        }

        public Builder rowKeyFieldNames(List<String> rowKeyFieldNames) {
            this.rowKeyFieldNames = rowKeyFieldNames;
            return this;
        }

        public Builder sortKeyFieldNames(List<String> sortKeyFieldNames) {
            this.sortKeyFieldNames = sortKeyFieldNames;
            return this;
        }

        public Builder valueFieldNames(List<String> valueFieldNames) {
            this.valueFieldNames = valueFieldNames;
            return this;
        }

        public SchemaWrapper build() {
            return new SchemaWrapper(this);
        }
    }
}
