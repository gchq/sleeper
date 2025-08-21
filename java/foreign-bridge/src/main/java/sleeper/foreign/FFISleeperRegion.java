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

import sleeper.foreign.bridge.FFIArray;

/**
 * A C ABI compatible representation of a Sleeper region.
 *
 * All arrays MUST be same length.
 */
@SuppressWarnings(value = {"checkstyle:membername"})
@SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class FFISleeperRegion extends Struct {
    /** Region partition region minimums. */
    public final FFIArray<Object> region_mins = new FFIArray<>(this);
    /** Region partition region maximums. MUST BE SAME LENGTH AS region_mins. */
    public final FFIArray<Object> region_maxs = new FFIArray<>(this);
    /** Region partition region minimums are inclusive? MUST BE SAME LENGTH AS region_mins. */
    public final FFIArray<java.lang.Boolean> region_mins_inclusive = new FFIArray<>(this);
    /** Region partition region maximums are inclusive? MUST BE SAME LENGTH AS region_mins. */
    public final FFIArray<java.lang.Boolean> region_maxs_inclusive = new FFIArray<>(this);

    public FFISleeperRegion(jnr.ffi.Runtime runtime) {
        super(runtime);
    }

    /**
     * Validate state of struct.
     *
     * @throws IllegalStateException when a invariant fails
     */
    public void validate() {
        region_mins.validate();
        region_maxs.validate();
        region_mins_inclusive.validate();
        region_maxs_inclusive.validate();

        // Check lengths
        long rowKeys = region_mins.length();
        if (rowKeys != region_maxs.length()) {
            throw new IllegalStateException("region maxs has length " + region_maxs.length() + " but there are " + rowKeys + " row key columns");
        }
        if (rowKeys != region_mins_inclusive.length()) {
            throw new IllegalStateException("region mins inclusive has length " + region_mins_inclusive.length() + " but there are " + rowKeys + " row key columns");
        }
        if (rowKeys != region_maxs_inclusive.length()) {
            throw new IllegalStateException("region maxs inclusive has length " + region_maxs_inclusive.length() + " but there are " + rowKeys + " row key columns");
        }
    }
}
