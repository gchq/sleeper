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

import org.apache.datasketches.quantiles.ItemsSketch;

import sleeper.core.schema.Field;
import sleeper.sketches.Sketches;

/**
 * Wraps a sketch object. Allows splitting the sketch into two when extending a partition tree over multiple levels.
 * Derives new views of the same sketch on either side of the split.
 */
public class WrappedSketchForSplitting implements SketchForSplitting {

    private final Field field;
    private final ItemsSketch<Object> sketch;
    private final long numberOfRows;
    private final double minQuantile;
    private final double medianQuantile;
    private final double maxQuantile;

    public WrappedSketchForSplitting(Field field, ItemsSketch<Object> sketch) {
        this(field, sketch, sketch.getN(), 0.0, 0.5, 1.0);
    }

    private WrappedSketchForSplitting(Field field, ItemsSketch<Object> sketch, long numberOfRows, double minQuantile, double medianQuantile, double maxQuantile) {
        this.field = field;
        this.sketch = sketch;
        this.numberOfRows = numberOfRows;
        this.minQuantile = minQuantile;
        this.medianQuantile = medianQuantile;
        this.maxQuantile = maxQuantile;
    }

    @Override
    public long getNumberOfRows() {
        return numberOfRows;
    }

    @Override
    public Object getMin() {
        return Sketches.readValueFromSketchWithWrappedBytes(sketch.getQuantile(minQuantile), field);
    }

    @Override
    public Object getMedian() {
        return Sketches.readValueFromSketchWithWrappedBytes(sketch.getQuantile(medianQuantile), field);
    }

    @Override
    public Object getMax() {
        return Sketches.readValueFromSketchWithWrappedBytes(sketch.getQuantile(maxQuantile), field);
    }

    /**
     * Derives a view of the sketch for the left side of a split on this field.
     *
     * @return the new sketch
     */
    public WrappedSketchForSplitting splitLeft() {
        double newMedian = (medianQuantile - minQuantile) / 2.0 + minQuantile;
        return new WrappedSketchForSplitting(field, sketch, numberOfRows / 2, minQuantile, newMedian, medianQuantile);
    }

    /**
     * Derives a view of the sketch for the right side of a split on this field.
     *
     * @return the new sketch
     */
    public WrappedSketchForSplitting splitRight() {
        double newMedian = (maxQuantile - medianQuantile) / 2.0 + medianQuantile;
        return new WrappedSketchForSplitting(field, sketch, numberOfRows / 2, medianQuantile, newMedian, maxQuantile);
    }

}
