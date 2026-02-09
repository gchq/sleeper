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
package sleeper.foreign;

import jnr.ffi.Struct;

import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.foreign.bridge.FFIArray;

import java.util.ArrayList;
import java.util.List;

/**
 * A C ABI compatible representation of a Sleeper region.
 *
 * All arrays MUST be same length.
 *
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects/sleeper_region.rs. The order and types of
 * the fields must match exactly.
 */
@SuppressWarnings(value = {"checkstyle:membername"})
public class FFISleeperRegion extends Struct {
    /** Region partition region minimums. */
    final FFIArray<Object> mins = new FFIArray<>(this);
    /** Region partition region maximums. MUST BE SAME LENGTH AS region_mins. */
    final FFIArray<Object> maxs = new FFIArray<>(this);
    /** Region partition region minimums are inclusive? MUST BE SAME LENGTH AS region_mins. */
    final FFIArray<java.lang.Boolean> mins_inclusive = new FFIArray<>(this);
    /** Region partition region maximums are inclusive? MUST BE SAME LENGTH AS region_mins. */
    final FFIArray<java.lang.Boolean> maxs_inclusive = new FFIArray<>(this);
    /**
     * Schema column indexes. Regions don't always have one range per row key column.
     * This array specifies column indexes into the schema that are specified by this region.
     */
    final FFIArray<java.lang.Integer> dimension_indexes = new FFIArray<>(this);

    public FFISleeperRegion(jnr.ffi.Runtime runtime) {
        super(runtime);
    }

    /**
     * Validate state of struct.
     *
     * @throws IllegalStateException when a invariant fails
     */
    void validate() {
        mins.validate();
        maxs.validate();
        mins_inclusive.validate();
        maxs_inclusive.validate();
        dimension_indexes.validate();

        // Check lengths
        long rowKeys = mins.length();
        if (rowKeys != maxs.length()) {
            throw new IllegalStateException("region maxs has length " + maxs.length() + " but there are " + rowKeys + " row keys in region");
        }
        if (rowKeys != mins_inclusive.length()) {
            throw new IllegalStateException("region mins inclusive has length " + mins_inclusive.length() + " but there are " + rowKeys + " row keys in region");
        }
        if (rowKeys != maxs_inclusive.length()) {
            throw new IllegalStateException("region maxs inclusive has length " + maxs_inclusive.length() + " but there are " + rowKeys + " row keys in region");
        }
        if (rowKeys != dimension_indexes.length()) {
            throw new IllegalStateException("region dimension indexes has length " + dimension_indexes.length() + " but there are " + rowKeys + " row keys in region");
        }
    }

    /**
     * Maps from a Sleeper region object.
     *
     * @param  region  the region
     * @param  schema  the schema
     * @param  runtime the FFI runtime
     * @return         the FFI region
     */
    public static FFISleeperRegion from(Region region, Schema schema, jnr.ffi.Runtime runtime) {
        FFISleeperRegion partitionRegion = new FFISleeperRegion(runtime);
        int numRanges = region.getRangesUnordered().size();
        List<Object> mins = new ArrayList<>(numRanges);
        List<java.lang.Boolean> minsInclusive = new ArrayList<>(numRanges);
        List<Object> maxs = new ArrayList<>(numRanges);
        List<java.lang.Boolean> maxsInclusive = new ArrayList<>(numRanges);
        List<java.lang.Integer> dimensionIndexes = new ArrayList<>(numRanges);
        List<Field> rowKeys = schema.getRowKeyFields();
        for (int dimension = 0; dimension < rowKeys.size(); dimension++) {
            Field field = rowKeys.get(dimension);
            Range range = region.getRange(field.getName());
            if (range == null) {
                continue;
            }
            mins.add(range.getMin());
            minsInclusive.add(range.isMinInclusive());
            maxs.add(range.getMax());
            maxsInclusive.add(range.isMaxInclusive());
            dimensionIndexes.add(dimension);
        }

        // Mins can't contain nulls
        partitionRegion.mins.populate(mins.toArray(), false);
        partitionRegion.mins_inclusive.populate(minsInclusive.toArray(java.lang.Boolean[]::new), false);
        // Maxs can contain nulls
        partitionRegion.maxs.populate(maxs.toArray(), true);
        partitionRegion.maxs_inclusive.populate(maxsInclusive.toArray(java.lang.Boolean[]::new), false);
        partitionRegion.dimension_indexes.populate(dimensionIndexes.toArray(java.lang.Integer[]::new), false);
        return partitionRegion;
    }

    /**
     * Maps to a Sleeper region object.
     *
     * @param  schema the schema
     * @return        the region
     */
    public Region toSleeperRegion(Schema schema) {
        validate();
        List<Field> rowKeys = schema.getRowKeyFields();
        RangeFactory rangeFactory = new RangeFactory(schema);
        java.lang.Boolean[] minsInclusive = mins_inclusive.readBack(java.lang.Boolean.class, false);
        java.lang.Boolean[] maxsInclusive = maxs_inclusive.readBack(java.lang.Boolean.class, false);
        java.lang.Integer[] dimensionIndexes = dimension_indexes.readBack(java.lang.Integer.class, false);
        List<Range> ranges = new ArrayList<>(minsInclusive.length);
        jnr.ffi.Runtime runtime = getRuntime();
        for (int i = 0; i < dimensionIndexes.length; i++) {
            Field field = rowKeys.get(dimensionIndexes[i]);
            PrimitiveType type = (PrimitiveType) field.getType();
            Object min = mins.getFieldValue(i, type, false, runtime);
            Object max = maxs.getFieldValue(i, type, true, runtime);
            ranges.add(rangeFactory.createRange(field, min, minsInclusive[i], max, maxsInclusive[i]));
        }
        return new Region(ranges);
    }
}
