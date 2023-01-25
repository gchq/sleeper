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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SchemaConverter {
    private SchemaConverter() {
    }

    /**
     * Create an Arrow Schema from a Sleeper Schema. The order of the fields in each Schema is retained.
     *
     * @param sleeperSchema The Sleeper {@link Schema}
     * @return The Arrow {@link org.apache.arrow.vector.types.pojo.Schema}
     */
    public static org.apache.arrow.vector.types.pojo.Schema convertSleeperSchemaToArrowSchema(Schema sleeperSchema) {
        List<org.apache.arrow.vector.types.pojo.Field> arrowFields =
                sleeperSchema.getAllFields().stream()
                        .map(FieldConverter::convertSleeperFieldToArrowField)
                        .collect(Collectors.toList());
        return new org.apache.arrow.vector.types.pojo.Schema(arrowFields);
    }

    public static Schema convertArrowSchemaToSleeperSchema(org.apache.arrow.vector.types.pojo.Schema arrowSchema,
                                                           List<String> rowKeyFieldNames,
                                                           List<String> sortKeyFieldNames,
                                                           List<String> valueFieldNames) {
        Map<String, Field> sleeperFields = new HashMap<>();
        arrowSchema.getFields().stream()
                .map(FieldConverter::convertArrowFieldToSleeperField)
                .forEach(field -> sleeperFields.put(field.getName(), field));

        return Schema.builder()
                .rowKeyFields(rowKeyFieldNames.stream()
                        .map(sleeperFields::get)
                        .collect(Collectors.toList()))
                .sortKeyFields(sortKeyFieldNames.stream()
                        .map(sleeperFields::get)
                        .collect(Collectors.toList()))
                .valueFields(valueFieldNames.stream()
                        .map(sleeperFields::get)
                        .collect(Collectors.toList()))
                .build();
    }
}
