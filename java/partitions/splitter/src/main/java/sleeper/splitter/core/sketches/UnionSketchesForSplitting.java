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

import org.apache.datasketches.quantiles.ItemsUnion;

import sleeper.core.schema.Field;
import sleeper.sketches.Sketches;

import java.util.List;
import java.util.function.UnaryOperator;

/**
 * Combines multiple sketches together to treat them as a single sketch.
 */
public class UnionSketchesForSplitting implements SketchesForSplitting {

    private final List<Sketches> sketches;

    public UnionSketchesForSplitting(List<Sketches> sketches) {
        this.sketches = sketches;
    }

    @Override
    public SketchForSplitting getSketch(Field field) {
        ItemsUnion<Object> union = Sketches.createUnion(field.getType(), 16384);
        for (Sketches sketch : sketches) {
            union.update(sketch.getQuantilesSketch(field.getName()));
        }
        return new WrappedSketchForSplitting(field, union.getResult());
    }

    @Override
    public WrappedSketchesForSplitting splitOnField(Field field, UnaryOperator<WrappedSketchForSplitting> split) {
        /*
         * Map<String, WrappedSketchForSplitting> newFieldNameToSketch = new HashMap<>(fieldNameToSketch);
         * newFieldNameToSketch.put(field.getName(), split.apply(fieldNameToSketch.get(field.getName())));
         * return new WrappedSketchesForSplitting(newFieldNameToSketch);
         */
        return null;
    }

}
