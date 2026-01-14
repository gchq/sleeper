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
package sleeper.splitter.core.sketches;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.sketches.Sketches;

import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Wraps a sketches object. Allows splitting a sketch into two when extending a partition tree over multiple levels.
 */
public class WrappedSketchesForSplitting implements SketchesForSplitting {

    private final Map<String, WrappedSketchForSplitting> fieldNameToSketch;

    private WrappedSketchesForSplitting(Map<String, WrappedSketchForSplitting> fieldNameToSketch) {
        this.fieldNameToSketch = fieldNameToSketch;
    }

    public static WrappedSketchesForSplitting from(Schema schema, Sketches sketches) {
        return new WrappedSketchesForSplitting(
                schema.getRowKeyFields().stream()
                        .collect(Collectors.toMap(Field::getName,
                                field -> new WrappedSketchForSplitting(field,
                                        sketches.getQuantilesSketch(field.getName())))));
    }

    @Override
    public SketchForSplitting getSketch(Field rowKeyField) {
        return fieldNameToSketch.get(rowKeyField.getName());
    }

    /**
     * Creates a view of a limited range of one of the sketches. This can be used when a partition has been split, to
     * derive sketches for the new child partitions.
     *
     * @param  field the field the partition was split on
     * @param  split how to derive the new sketch for the field from the old parent partition's sketch (e.g. get the
     *               left or right side of the split)
     * @return       the new sketches
     */
    public WrappedSketchesForSplitting splitOnField(Field field, UnaryOperator<WrappedSketchForSplitting> split) {
        Map<String, WrappedSketchForSplitting> newFieldNameToSketch = new HashMap<>(fieldNameToSketch);
        newFieldNameToSketch.put(field.getName(), split.apply(fieldNameToSketch.get(field.getName())));
        return new WrappedSketchesForSplitting(newFieldNameToSketch);
    }

}
