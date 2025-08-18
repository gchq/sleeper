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
import sleeper.core.range.Region;
import sleeper.foreign.bridge.FFIArray;

/**
 * A C ABI compatible representation of a Sleeper region. If you updated
 * this struct (field ordering, types, etc.), you MUST update the corresponding Rust definition
 * in rust/sleeper_df/src/objects.rs. The order and types of the fields must match exactly.
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

    /**
     * Creates and validates Sleeper region into an FFI compatible form.
     *
     * @param  runtime               FFI runtime
     * @throws IllegalStateException when the region data is invalid
     */
    @SuppressWarnings(value = "checkstyle:avoidNestedBlocks")
    public FFISleeperRegion(jnr.ffi.Runtime runtime, Region region) {
        super(runtime);
        // Extra braces: Make sure wrong array isn't populated to wrong pointers
        {
            // This array can't contain nulls
            Object[] regionMins = region.getRanges().stream().map(Range::getMin).toArray();
            this.mins.populate(regionMins, false);
        }
        {
            java.lang.Boolean[] regionMinInclusives = region.getRanges().stream().map(Range::isMinInclusive)
                    .toArray(java.lang.Boolean[]::new);
            this.mins_inclusive.populate(regionMinInclusives, false);
        }
        {
            // This array can contain nulls
            Object[] regionMaxs = region.getRanges().stream().map(Range::getMax).toArray();
            this.maxs.populate(regionMaxs, true);
        }
        {
            java.lang.Boolean[] regionMaxInclusives = region.getRanges().stream().map(Range::isMaxInclusive)
                    .toArray(java.lang.Boolean[]::new);
            this.maxs_inclusive.populate(regionMaxInclusives, false);
        }
        validate();
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
}
