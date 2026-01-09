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
import sleeper.core.range.RangeCanonicaliser;
import sleeper.core.schema.Field;
import sleeper.core.schema.type.PrimitiveType;

import java.util.Optional;

/**
 * Utility methods to intersect Ranges.
 */
public class RangeIntersector {
    private RangeIntersector() {

    }

    /**
     * Intersects the two provided ranges. If the two ranges do not overlap, this will return an empty optional.
     *
     * @param  range1       the first range
     * @param  range2       the second range
     * @param  rangeFactory the RangeFactory to use
     * @return              an optional range corresponding to the intersection of the two provided ranges
     */
    public static Optional<Range> intersectRanges(Range range1, Range range2, RangeFactory rangeFactory) {
        if (!range1.getField().equals(range2.getField())) {
            throw new IllegalArgumentException("Cannot intersect two ranges for different fields");
        }

        if (!range1.doesRangeOverlap(range2)) {
            return Optional.empty();
        }

        // Min is inclusive, max is exclusive when in canonical form
        Range canonicalRange1 = RangeCanonicaliser.canonicaliseRange(range1);
        Range canonicalRange2 = RangeCanonicaliser.canonicaliseRange(range2);

        Comparable canonicalRange1Min = validateComparable(range1.getField(), canonicalRange1.getMin(), "minimum value");
        Comparable canonicalRange2Min = validateComparable(range1.getField(), canonicalRange2.getMin(), "minimum value");
        Object newMin = PrimitiveType.COMPARATOR.compare(canonicalRange1Min, canonicalRange2Min) < 0 ? canonicalRange2Min : canonicalRange1Min;

        Comparable canonicalRange1Max = validateComparable(range1.getField(), canonicalRange1.getMax(), "maximum value");
        Comparable canonicalRange2Max = validateComparable(range1.getField(), canonicalRange2.getMax(), "maximum value");
        Object newMax = PrimitiveType.COMPARATOR.compare(canonicalRange1Max, canonicalRange2Max) > 0 ? canonicalRange2Max : canonicalRange1Max;

        return Optional.of(rangeFactory.createRange(range1.getField(), newMin, true, newMax, false));
    }

    private static Comparable validateComparable(Field field, Object value, String description) {
        try {
            PrimitiveType type = (PrimitiveType) field.getType();
            return type.toComparable(value);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Could not read " + description + " for field " + field.getName(), e);
        }
    }
}
