/*
 * Copyright 2022-2024 Crown Copyright
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

import com.facebook.collections.ByteArray;

import sleeper.core.key.Key;
import sleeper.core.record.KeyComparator;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a contiguous range in a single dimension.
 */
public class Range {
    private final Field field;
    private final Object min;
    private final boolean minInclusive;
    private final Object max;
    private final boolean maxInclusive;

    private Range(Field field, Object min, boolean minInclusive, Object max, boolean maxInclusive) {
        this.field = field;
        this.min = min;
        this.minInclusive = minInclusive;
        this.max = max;
        this.maxInclusive = maxInclusive;
        if (min == null) {
            throw new IllegalArgumentException("Minimum value must not be null for field " + field.getName());
        }
        if (maxInclusive && max == null) {
            throw new IllegalArgumentException("Maximum value cannot be inclusive null for field " + field.getName());
        }
        Comparable minComparable = validateComparable(field, min, "minimum value");
        Comparable maxComparable = validateComparable(field, max, "maximum value");
        if (maxComparable != null && PrimitiveType.COMPARATOR.compare(minComparable, maxComparable) > 0) {
            throw new IllegalArgumentException("Range of field " + field.getName() + " has minimum greater than maximum, " + minComparable + " > " + maxComparable);
        }
    }

    private static Comparable validateComparable(Field field, Object value, String description) {
        try {
            PrimitiveType type = (PrimitiveType) field.getType();
            return type.toComparable(value);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Could not read " + description + " for field " + field.getName(), e);
        }
    }

    public Range(Field field, Object min, Object max) {
        this(field, min, true, max, false);
    }

    public Field getField() {
        return field;
    }

    public String getFieldName() {
        return field.getName();
    }

    public Type getFieldType() {
        return field.getType();
    }

    public Object getMin() {
        return min;
    }

    public boolean isMinInclusive() {
        return minInclusive;
    }

    public Object getMax() {
        return max;
    }

    public boolean isMaxInclusive() {
        return maxInclusive;
    }

    /**
     * Checks whether the provided value is contained within this range.
     *
     * @param  value the value to check
     * @return       whether the object is contained within this range
     */
    public boolean doesRangeContainObject(Object value) {
        Type type = field.getType();
        if (type instanceof IntType) {
            if (!(value instanceof Integer)) {
                throw new IllegalArgumentException("The object must match the schema: expected an Integer, got " + value);
            }
            return doesRangeContainInt((Integer) value);
        } else if (type instanceof LongType) {
            if (!(value instanceof Long)) {
                throw new IllegalArgumentException("The object must match the schema: expected a Long, got " + value);
            }
            return doesRangeContainLong((Long) value);
        } else if (type instanceof StringType) {
            if (!(value instanceof String)) {
                throw new IllegalArgumentException("The object must match the schema: expected a String, got " + value);
            }
            return doesRangeContainString((String) value);
        } else if (type instanceof ByteArrayType) {
            if (!(value instanceof byte[])) {
                throw new IllegalArgumentException("The object must match the schema: expected a byte[], got " + value);
            }
            return doesRangeContainByteArray((byte[]) value);
        }
        throw new IllegalArgumentException("Unknown type in the schema: " + type);
    }

    /**
     * Checks whether the provided range overlaps with this range.
     *
     * @param  otherRange the range to check
     * @return            whether the range overlaps with this range
     */
    public boolean doesRangeOverlap(Range otherRange) {
        // We work on the canonicalised version of the ranges as this makes the following
        // logic simpler. As an example of a counter-intuitive example, consider whether
        // the range (1,5) overlaps with the range (4,6) (where the ends are not inclusive).
        // If this range is for an integer field then these ranges do not overlap:
        // 5 is not in (1,5), but 4 is, 4 is not in (4,6), but 5 is. If this was a string
        // field then they do overlap (e.g. the string "4zzzz" is in both ranges).

        Range canonicalRange = RangeCanonicaliser.canonicaliseRange(this);
        Range canonicalOtherRange = RangeCanonicaliser.canonicaliseRange(otherRange);

        // The otherRange doesn't overlap this range if it is either completely
        // to the left of the partition or completely to the right of the
        // partition:
        //      Range:                             |------------)
        //      Non-overlapping range:    |------|
        //      Non-overlapping range:      |------) (ranges in canonical form do not include their right-most boundary so if range max is equal to partition min then there is no overlap)
        //      Non-overlapping range:                            |------)
        //      Non-overlapping range:                          |------) (partitions do not include their right-most boundary so if range min is equal to partition max then there is no overlap)
        //      Overlapping range:                 |----)
        //      Overlapping range:                 |------------)
        //      Overlapping range:          |-------)
        //      Overlapping range:         |-------------)
        //      Overlapping range:                           |-------------)

        KeyComparator keyComparator = new KeyComparator((PrimitiveType) field.getType());

        // Other range to the left of this one
        boolean otherRangeMaxLessThanRangeMin = keyComparator.compare(Key.create(canonicalOtherRange.max), Key.create(canonicalRange.min)) <= 0;
        if (otherRangeMaxLessThanRangeMin) {
            return false;
        }

        // Other range to the right of this one
        // Region right of partition case:
        //  max of partition <= min of range
        boolean otherRangeMinGreaterThanRangeMax = keyComparator.compare(Key.create(canonicalRange.max), Key.create(canonicalOtherRange.min)) <= 0;
        if (otherRangeMinGreaterThanRangeMax) {
            return false;
        }

        return true;
    }

    private boolean doesRangeContainInt(Integer value) {
        Integer minInteger = (Integer) min;

        // If min is inclusive then return false if value is less than the minimum of the range
        if (minInclusive) {
            if (value < minInteger) {
                return false;
            }
        } else {
            // If min is not inclusive then return false if value is less than or equal to the minimum of the range
            if (value <= minInteger) {
                return false;
            }
        }

        // If the max is null then the value is less than the max
        if (null == max) {
            return true;
        }

        Integer maxInteger = (Integer) max;
        // If max is inclusive then return false if value is greater than the maximum of the range
        if (maxInclusive) {
            if (value > maxInteger) {
                return false;
            }
        } else {
            // If max is not inclusive then return false if value is greater than or equal to the maximum of the range
            if (value >= maxInteger) {
                return false;
            }
        }

        return true;
    }

    private boolean doesRangeContainLong(Long value) {
        Long minLong = (Long) min;

        // If min is inclusive then return false if value is less than the minimum of the range
        if (minInclusive) {
            if (value < minLong) {
                return false;
            }
        } else {
            // If min is not inclusive then return false if value is less than or equal to the minimum of the range
            if (value <= minLong) {
                return false;
            }
        }

        // If the max is null then the value is less than the max
        if (null == max) {
            return true;
        }

        Long maxLong = (Long) max;
        // If max is inclusive then return false if value is greater than the maximum of the range
        if (maxInclusive) {
            if (value > maxLong) {
                return false;
            }
        } else {
            // If max is not inclusive then return false if value is greater than or equal to the maximum of the range
            if (value >= maxLong) {
                return false;
            }
        }

        return true;
    }

    private boolean doesRangeContainString(String value) {
        String minString = (String) min;

        // If min is inclusive then return false if value is less than the minimum of the range
        if (minInclusive) {
            if (value.compareTo(minString) < 0) {
                return false;
            }
        } else {
            // If min is not inclusive then return false if value is less than or equal to the minimum of the range
            if (value.compareTo(minString) <= 0) {
                return false;
            }
        }

        // If the max is null then the value is less than the max
        if (null == max) {
            return true;
        }

        String maxString = (String) max;
        // If max is inclusive then return false if value is greater than the maximum of the range
        if (maxInclusive) {
            if (value.compareTo(maxString) > 0) {
                return false;
            }
        } else {
            // If max is not inclusive then return false if value is greater than or equal to the maximum of the range
            if (value.compareTo(maxString) >= 0) {
                return false;
            }
        }

        return true;
    }

    private boolean doesRangeContainByteArray(byte[] value) {
        ByteArray valueByteArray = ByteArray.wrap(value);
        ByteArray minByteArray = ByteArray.wrap((byte[]) min);

        // If min is inclusive then return false if value is less than the minimum of the range
        if (minInclusive) {
            if (valueByteArray.compareTo(minByteArray) < 0) {
                return false;
            }
        } else {
            // If min is not inclusive then return false if value is less than or equal to the minimum of the range
            if (valueByteArray.compareTo(minByteArray) <= 0) {
                return false;
            }
        }

        // If the max is null then the value is less than the max
        if (null == max) {
            return true;
        }

        ByteArray maxByteArray = ByteArray.wrap((byte[]) max);
        // If max is inclusive then return false if value is greater than the maximum of the range
        if (maxInclusive) {
            if (valueByteArray.compareTo(maxByteArray) > 0) {
                return false;
            }
        } else {
            // If max is not inclusive then return false if value is greater than or equal to the maximum of the range
            if (valueByteArray.compareTo(maxByteArray) >= 0) {
                return false;
            }
        }

        return true;
    }

    public boolean isInCanonicalForm() {
        return isMinInclusive() && !isMaxInclusive();
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 13 * hash + Objects.hashCode(this.field);
        hash = 13 * hash + (this.minInclusive ? 1 : 0);
        hash = 13 * hash + (this.maxInclusive ? 1 : 0);
        if (field.getType() instanceof ByteArrayType) {
            hash = 13 * hash + Objects.hashCode(ByteArray.wrap((byte[]) this.min));
            hash = 13 * hash + Objects.hashCode(ByteArray.wrap((byte[]) this.max));
        } else {
            hash = 13 * hash + Objects.hashCode(this.min);
            hash = 13 * hash + Objects.hashCode(this.max);
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Range other = (Range) obj;

        Object minTransformed;
        Object otherMinTransformed;
        Object maxTransformed;
        Object otherMaxTransformed;
        if (field.getType() instanceof ByteArrayType) {
            minTransformed = ByteArray.wrap((byte[]) this.min);
            otherMinTransformed = ByteArray.wrap((byte[]) other.min);
            maxTransformed = ByteArray.wrap((byte[]) this.max);
            otherMaxTransformed = ByteArray.wrap((byte[]) other.max);
        } else {
            minTransformed = this.min;
            otherMinTransformed = other.min;
            maxTransformed = this.max;
            otherMaxTransformed = other.max;
        }
        if (this.minInclusive != other.minInclusive) {
            return false;
        }
        if (this.maxInclusive != other.maxInclusive) {
            return false;
        }
        if (!Objects.equals(this.field, other.field)) {
            return false;
        }
        if (!Objects.equals(minTransformed, otherMinTransformed)) {
            return false;
        }
        if (!Objects.equals(maxTransformed, otherMaxTransformed)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Range{" + "field=" + field + ", min=" + min + ", minInclusive=" + minInclusive + ", max=" + max + ", maxInclusive=" + maxInclusive + '}';
    }

    /**
     * Creates ranges and validates them against a schema.
     */
    public static class RangeFactory {
        private final Map<String, PrimitiveType> rowKeyFieldToType;
        private final Set<String> rowKeyFieldNames;

        public RangeFactory(Schema schema) {
            this.rowKeyFieldToType = new HashMap<>();
            for (Field field : schema.getRowKeyFields()) {
                this.rowKeyFieldToType.put(field.getName(), (PrimitiveType) field.getType());
            }
            this.rowKeyFieldNames = new HashSet<>(schema.getRowKeyFieldNames());
        }

        /**
         * Creates a new range.
         *
         * @param  field        the field which the range applies to
         * @param  min          the minimum of the range
         * @param  minInclusive whether the minimum is inclusive or not
         * @param  max          the maximum of the range
         * @param  maxInclusive whether the maximum is inclusive or not
         * @return              the new range
         */
        public Range createRange(Field field, Object min, boolean minInclusive, Object max, boolean maxInclusive) {
            // fieldName should be a row key
            if (!rowKeyFieldNames.contains(field.getName())) {
                throw new IllegalArgumentException("Field name should be a row key field, got " + field.getName() + ", row key fields are " + rowKeyFieldNames);
            }
            return new Range(field, min, minInclusive, max, maxInclusive);
        }

        /**
         * Creates a new range.
         *
         * @param  fieldName    the name of the field which the range applies to
         * @param  min          the minimum of the range
         * @param  minInclusive whether the minimum is inclusive or not
         * @param  max          the maximum of the range
         * @param  maxInclusive whether the maximum is inclusive or not
         * @return              the new range
         */
        public Range createRange(String fieldName, Object min, boolean minInclusive, Object max, boolean maxInclusive) {
            return createRange(new Field(fieldName, rowKeyFieldToType.get(fieldName)), min, minInclusive, max, maxInclusive);
        }

        /**
         * Creates a new range.
         *
         * @param  field the field which the range applies to
         * @param  min   the minimum of the range (inclusive)
         * @param  max   the maximum of the range (exclusive)
         * @return       the new range
         */
        public Range createRange(Field field, Object min, Object max) {
            return createRange(field, min, true, max, false);
        }

        /**
         * Creates a new range.
         *
         * @param  fieldName the name of the field which the range applies to
         * @param  min       the minimum of the range (inclusive)
         * @param  max       the maximum of the range (exclusive)
         * @return           the new range
         */
        public Range createRange(String fieldName, Object min, Object max) {
            return createRange(fieldName, min, true, max, false);
        }

        /**
         * Creates an exact range (where the min and max values are the same).
         *
         * @param  field the field which the range applies to
         * @param  value the value for the exact range
         * @return       the new range
         */
        public Range createExactRange(Field field, Object value) {
            return createRange(field, value, true, value, true);
        }

        /**
         * Creates an exact range (where the min and max values are the same).
         *
         * @param  fieldName the name of the field which the range applies to
         * @param  value     the value for the exact range
         * @return           the new range
         */
        public Range createExactRange(String fieldName, Object value) {
            return createRange(fieldName, value, true, value, true);
        }
    }
}
