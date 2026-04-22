/*
 * Copyright 2022-2026 Crown Copyright
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
import jnr.ffi.TypeAlias;

import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.PrimitiveType;

import java.util.ArrayList;
import java.util.List;

/**
 * A C ABI compatible representation of a Sleeper region.
 *
 * All arrays MUST be same length.
 *
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects/sleeper_region.rs. The order and
 * types of
 * the fields must match exactly.
 */
@SuppressWarnings(value = {"checkstyle:membername"})
public class FFISleeperRegion extends Struct {
    /** Length of arrays. All assumed to be same length. */
    final Struct.size_t len = new Struct.size_t();
    /** Region partition region minimums. May not contain "Empty" elements. */
    final Struct.StructRef<FFIElement> mins = new Struct.StructRef<>(FFIElement.class);
    /** Prevent GC. */
    FFIElement[] java_mins;
    /** Region partition region maximums. May contain "Empty" elements. */
    final Struct.StructRef<FFIElement> maxs = new Struct.StructRef<>(FFIElement.class);
    /** Prevent GC. */
    FFIElement[] java_maxs;
    /** Region partition region minimums are inclusive? MUST BE SAME LENGTH AS region_mins. */
    final Struct.Pointer mins_inclusive = new Struct.Pointer();
    /** Pointer to allocated native memory. Prevents GC of memory until this object is collected. */
    jnr.ffi.Pointer java_mins_inclusive;
    /** Region partition region maximums are inclusive? MUST BE SAME LENGTH AS region_mins. */
    final Struct.Pointer maxs_inclusive = new Struct.Pointer();
    /** Pointer to allocated native memory. Prevents GC of memory until this object is collected. */
    jnr.ffi.Pointer java_maxs_inclusive;
    /**
     * Schema column indexes. Regions don't always have one range per row key column.
     * This array specifies column indexes into the schema that are specified by this region.
     */
    final Struct.Pointer dimension_indexes = new Struct.Pointer();
    /** Pointer to allocated native memory. Prevents GC of memory until this object is collected. */
    jnr.ffi.Pointer java_dimension_indexes;

    public FFISleeperRegion(jnr.ffi.Runtime runtime) {
        super(runtime);
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

        int allLength = mins.size();

        // Check no nulls in mins
        if (mins.stream().anyMatch(o -> o == null)) {
            throw new IllegalArgumentException("Region minimums cannot contain nulls");
        }

        FFISleeperRegion partitionRegion = new FFISleeperRegion(runtime);
        partitionRegion.len.set(allLength);
        // Convert minimums to FFIElement objects
        FFIElement[] minArray = mins.stream()
                .map(o -> new FFIElement(runtime, o))
                .toArray(FFIElement[]::new);
        partitionRegion.mins.set(minArray);
        partitionRegion.java_mins = minArray;

        // Convert maximums to FFIElement objects
        FFIElement[] maxArray = maxs.stream()
                .map(o -> new FFIElement(runtime, o))
                .toArray(FFIElement[]::new);
        partitionRegion.maxs.set(maxArray);
        partitionRegion.java_maxs = maxArray;

        jnr.ffi.Pointer nativeMinsInclusive = runtime.getMemoryManager().allocateDirect(allLength);
        for (int i = 0; i < minsInclusive.size(); i++) {
            boolean inclusive = minsInclusive.get(i);
            nativeMinsInclusive.putByte(i, inclusive ? (byte) 1 : (byte) 0);
        }
        partitionRegion.mins_inclusive.set(nativeMinsInclusive);
        partitionRegion.java_mins_inclusive = nativeMinsInclusive;

        jnr.ffi.Pointer nativeMaxsInclusive = runtime.getMemoryManager().allocateDirect(allLength);
        for (int i = 0; i < maxsInclusive.size(); i++) {
            boolean inclusive = maxsInclusive.get(i);
            nativeMaxsInclusive.putByte(i, inclusive ? (byte) 1 : (byte) 0);
        }
        partitionRegion.maxs_inclusive.set(nativeMaxsInclusive);
        partitionRegion.java_maxs_inclusive = nativeMaxsInclusive;

        int size_tBytes = runtime.findType(TypeAlias.size_t).size();
        jnr.ffi.Pointer nativeDimensionIndexes = runtime.getMemoryManager().allocate(
                size_tBytes * allLength);
        for (int i = 0; i < dimensionIndexes.size(); i++) {
            int dimensionIndex = dimensionIndexes.get(i);
            writeCSize_t(size_tBytes, nativeDimensionIndexes, i, dimensionIndex);
        }
        partitionRegion.dimension_indexes.set(nativeDimensionIndexes);
        partitionRegion.java_dimension_indexes = nativeDimensionIndexes;

        return partitionRegion;
    }

    private static void writeCSize_t(int size_tBytes, jnr.ffi.Pointer base, long offset, int value) {
        if (size_tBytes == 4) {
            base.putInt(offset, value);
        } else if (size_tBytes == 8) {
            base.putLong(offset, value);
        } else {
            throw new IllegalArgumentException("size_tBytes must be 4 or 8");
        }
    }

    /**
     * Maps to a Sleeper region object.
     *
     * @param  schema the schema
     * @return        the region
     */
    public Region toSleeperRegion(Schema schema) {
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
