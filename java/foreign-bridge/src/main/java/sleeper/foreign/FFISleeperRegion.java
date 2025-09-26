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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
 */
@SuppressWarnings(value = {"checkstyle:membername"})
@SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class FFISleeperRegion extends Struct {
    /** Region partition region minimums. */
    public final FFIArray<Object> mins = new FFIArray<>(this);
    /** Region partition region maximums. MUST BE SAME LENGTH AS region_mins. */
    public final FFIArray<Object> maxs = new FFIArray<>(this);
    /** Region partition region minimums are inclusive? MUST BE SAME LENGTH AS region_mins. */
    public final FFIArray<java.lang.Boolean> mins_inclusive = new FFIArray<>(this);
    /** Region partition region maximums are inclusive? MUST BE SAME LENGTH AS region_mins. */
    public final FFIArray<java.lang.Boolean> maxs_inclusive = new FFIArray<>(this);

    public FFISleeperRegion(jnr.ffi.Runtime runtime) {
        super(runtime);
    }

    /**
     * Validate state of struct.
     *
     * @throws IllegalStateException when a invariant fails
     */
    public void validate() {
        mins.validate();
        maxs.validate();
        mins_inclusive.validate();
        maxs_inclusive.validate();

        // Check lengths
        long rowKeys = mins.length();
        if (rowKeys != maxs.length()) {
            throw new IllegalStateException("region maxs has length " + maxs.length() + " but there are " + rowKeys + " row key columns");
        }
        if (rowKeys != mins_inclusive.length()) {
            throw new IllegalStateException("region mins inclusive has length " + mins_inclusive.length() + " but there are " + rowKeys + " row key columns");
        }
        if (rowKeys != maxs_inclusive.length()) {
            throw new IllegalStateException("region maxs inclusive has length " + maxs_inclusive.length() + " but there are " + rowKeys + " row key columns");
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
        List<Range> orderedRanges = region.getRangesOrdered(schema);
        // This array can't contain nulls
        partitionRegion.mins.populate(orderedRanges.stream().map(Range::getMin).toArray(), false);
        partitionRegion.mins_inclusive.populate(
                orderedRanges.stream().map(Range::isMinInclusive).toArray(java.lang.Boolean[]::new), false);
        // This array can contain nulls
        partitionRegion.maxs.populate(orderedRanges.stream().map(Range::getMax).toArray(), true);
        partitionRegion.maxs_inclusive.populate(orderedRanges.stream().map(Range::isMaxInclusive)
                .toArray(java.lang.Boolean[]::new), false);
        partitionRegion.validate();
        return partitionRegion;
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
        List<Range> ranges = new ArrayList<>(minsInclusive.length);
        jnr.ffi.Runtime runtime = getRuntime();
        for (int i = 0; i < rowKeys.size(); i++) {
            Field field = rowKeys.get(i);
            PrimitiveType type = (PrimitiveType) field.getType();
            Object min = mins.getFieldValue(i, type, false, runtime);
            Object max = maxs.getFieldValue(i, type, true, runtime);
            ranges.add(rangeFactory.createRange(field, min, minsInclusive[i], max, maxsInclusive[i]));
        }
        return new Region(ranges);
    }
}
