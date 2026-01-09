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
package sleeper.spark;

import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Utility methods to intersect Regions.
 */
public class RegionIntersector {
    private RegionIntersector() {

    }

    /**
     * Intersects the provided list of regions. If they do not overlap an empty optional will be returned.
     *
     * @param  regions      the regions to be intersected
     * @param  rangeFactory a range factory
     * @param  schema       the schema of the Sleeper table
     * @return              an optional region formed from intersecting the provided regions
     */
    public static Optional<Region> intersectRegions(List<Region> regions, RangeFactory rangeFactory, Schema schema) {
        if (null == regions || regions.isEmpty()) {
            return Optional.empty();
        }
        Optional<Region> intersectedRegion = Optional.of(regions.get(0));
        if (regions.size() == 1) {
            return intersectedRegion;
        }
        for (int i = 1; i < regions.size(); i++) {
            intersectedRegion = intersectOptionalRegions(intersectedRegion, Optional.of(regions.get(i)), rangeFactory, schema);
        }
        return intersectedRegion;
    }

    private static Optional<Region> intersectOptionalRegions(Optional<Region> optionalRegion1, Optional<Region> optionalRegion2,
            RangeFactory rangeFactory, Schema schema) {
        if (optionalRegion1.isEmpty() && optionalRegion2.isEmpty()) {
            return Optional.empty();
        }
        if (optionalRegion1.isPresent() && optionalRegion2.isEmpty()) {
            return optionalRegion1;
        }
        if (optionalRegion1.isEmpty() && optionalRegion2.isPresent()) {
            return optionalRegion2;
        }
        return intersectRegions(optionalRegion1.get(), optionalRegion2.get(), rangeFactory, schema);
    }

    /**
     * Intersects the provided two regions. If they do not overlap an empty optional will be returned.
     *
     * @param  region1      the first region
     * @param  region2      the second region
     * @param  rangeFactory a range factory
     * @param  schema       the schema of the Sleeper table
     * @return              an optional region formed from intersecting the provided regions
     */
    public static Optional<Region> intersectRegions(Region region1, Region region2, RangeFactory rangeFactory, Schema schema) {
        if (!region1.doesRegionOverlap(region2)) {
            return Optional.empty();
        }

        List<Range> ranges = new ArrayList<>();
        for (Field rowKeyField : schema.getRowKeyFields()) {
            Range range1 = region1.getRange(rowKeyField.getName());
            Range range2 = region2.getRange(rowKeyField.getName());
            Optional<Range> intersectedRange = RangeIntersector.intersectRanges(range1, range2, rangeFactory);
            if (intersectedRange.isEmpty()) {
                throw new RuntimeException("Regions " + region1 + " and " + region2
                        + " had two ranges that do not intersect (" + range1 + ", " + range2 + ") - this is impossible");
            }
            ranges.add(intersectedRange.get());
        }
        return Optional.of(new Region(ranges));
    }
}
