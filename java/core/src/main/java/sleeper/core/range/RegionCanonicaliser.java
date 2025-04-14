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
package sleeper.core.range;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts each range in a region into canonical form. Uses {@link RangeCanonicaliser}.
 */
public class RegionCanonicaliser {

    private RegionCanonicaliser() {
    }

    /**
     * Converts each range in a region into canonical form.
     *
     * @param  region the region to canonicalise
     * @return        a new region with all ranges canonicalised
     */
    public static Region canonicaliseRegion(Region region) {
        if (isRegionInCanonicalForm(region)) {
            return region;
        }

        List<Range> ranges = region.getRanges();
        List<Range> canonicalisedRanges = new ArrayList<>();
        for (Range range : ranges) {
            canonicalisedRanges.add(RangeCanonicaliser.canonicaliseRange(range));
        }

        return new Region(canonicalisedRanges);
    }

    /**
     * Checks whether all ranges in a region are in canonical form.
     *
     * @param  region the region to check
     * @return        whether all ranges in the region are in canonical form
     */
    public static boolean isRegionInCanonicalForm(Region region) {
        for (Range range : region.getRanges()) {
            if (!range.isInCanonicalForm()) {
                return false;
            }
        }
        return true;
    }
}
