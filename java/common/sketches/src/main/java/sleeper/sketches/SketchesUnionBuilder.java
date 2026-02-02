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
package sleeper.sketches;

import org.apache.datasketches.quantiles.ItemsUnion;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;

import java.util.Map;
import java.util.Map.Entry;

import static java.util.stream.Collectors.toMap;

/**
 * Creates sketches from a union of a number of other sketches.
 */
public class SketchesUnionBuilder {

    private final Schema schema;
    private final Map<String, ItemsUnion<Object>> fieldNameToUnion;

    public SketchesUnionBuilder(Schema schema) {
        this.schema = schema;
        this.fieldNameToUnion = schema.getRowKeyFields().stream()
                .collect(toMap(
                        Field::getName,
                        field -> Sketches.createUnion(field.getType())));
    }

    /**
     * Adds sketches into the union. The sketches must include an item sketch for each row key in the Sleeper table
     * schema.
     *
     * @param sketches the sketches to add
     */
    public void add(Sketches sketches) {
        for (Field field : schema.getRowKeyFields()) {
            ItemsUnion<Object> union = fieldNameToUnion.get(field.getName());
            union.update(sketches.getQuantilesSketch(field.getName()));
        }
    }

    /**
     * Gathers the results of the union into a sketches object.
     *
     * @return sketches
     */
    public Sketches build() {
        return new Sketches(schema, fieldNameToUnion.entrySet().stream()
                .collect(toMap(Entry::getKey, entry -> entry.getValue().getResult())));
    }
}
