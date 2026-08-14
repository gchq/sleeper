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
package sleeper.athena.record;

import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;

import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builds the Sleeper regions that a leaf partition query needs from the Athena split bounds and the
 * query's constraints. This is used by the DataFusion read path to push row-key filtering down into the
 * DataFusion engine.
 * <p>
 * Row-key predicates are translated to regions . Sort and value-key predicates will be applied as SQL.
 * <p>
 * An OR-style predicate (a multi-value {@code IN} list or a set of disjoint ranges) on the first row key is
 * expanded into one region per value/range. OR predicates on other row keys are not expanded and are left
 * covering all values; Athena will apply that constraint itself. The same fallback applies to deny-lists on
 * any row key.
 */
public class AthenaRegionFactory {
    private final Schema schema;
    private final RangeFactory rangeFactory;

    public AthenaRegionFactory(Schema schema) {
        this.schema = schema;
        this.rangeFactory = new RangeFactory(schema);
    }

    /**
     * Builds the region describing the bounds of the leaf partition being read. The min/max lists hold the
     * decoded row-key bounds from the split, one entry per row-key field in schema order.
     *
     * @param  minRowKeys the decoded minimum row-key values
     * @param  maxRowKeys the decoded maximum row-key values
     * @return            the partition region
     */
    public Region partitionRegion(List<Object> minRowKeys, List<Object> maxRowKeys) {
        List<Field> rowKeyFields = schema.getRowKeyFields();
        List<Range> ranges = new ArrayList<>();
        for (int i = 0; i < rowKeyFields.size(); i++) {
            Field field = rowKeyFields.get(i);
            ranges.add(rangeFactory.createRange(field, minRowKeys.get(i), true, maxRowKeys.get(i), false));
        }
        return new Region(ranges);
    }

    /**
     * Translates the row-key entries of the Athena constraints into query regions. Non-row-key entries are
     * ignored (they are pushed as SQL instead). If no row-key field carries a representable predicate, the
     * partition region is returned unchanged so the whole partition is scanned.
     *
     * @param  constraints     the Athena constraint summary
     * @param  partitionRegion the partition region
     * @return                 the query regions to push into DataFusion
     */
    public List<Region> queryRegions(Map<String, ValueSet> constraints, Region partitionRegion) {
        List<Field> rowKeyFields = schema.getRowKeyFields();
        boolean anyConstrained = false;

        // The first row key may expand an OR predicate into several ranges; the second or later row keys can produce
        // at most one.
        List<Range> firstKeyRanges;
        Field firstField = rowKeyFields.get(0);
        ValueSet firstValueSet = constraints == null ? null : constraints.get(firstField.getName());
        List<Range> expanded = firstValueSet == null ? null : rowKeyRanges(firstField, firstValueSet);
        if (expanded == null || expanded.isEmpty()) {
            firstKeyRanges = List.of(rangeFactory.createRangeCoveringAllValues(firstField));
        } else {
            firstKeyRanges = expanded;
            anyConstrained = true;
        }

        List<Range> otherRanges = new ArrayList<>();
        for (int i = 1; i < rowKeyFields.size(); i++) {
            Field field = rowKeyFields.get(i);
            ValueSet valueSet = constraints == null ? null : constraints.get(field.getName());
            Range range = valueSet == null ? null : rowKeyRange(field, valueSet);
            if (range == null) {
                otherRanges.add(rangeFactory.createRangeCoveringAllValues(field));
            } else {
                otherRanges.add(range);
                anyConstrained = true;
            }
        }

        if (!anyConstrained) {
            return List.of(partitionRegion);
        }

        List<Region> regions = new ArrayList<>();
        for (Range firstRange : firstKeyRanges) {
            List<Range> ranges = new ArrayList<>();
            ranges.add(firstRange);
            ranges.addAll(otherRanges);
            regions.add(new Region(ranges));
        }
        return regions;
    }

    // Translates the first row-key value set into one range per OR term (multi-value IN list or disjoint
    // ranges), or null if the predicate cannot be represented as a list of ranges.
    private List<Range> rowKeyRanges(Field field, ValueSet valueSet) {
        if (valueSet instanceof SortedRangeSet) {
            List<com.amazonaws.athena.connector.lambda.domain.predicate.Range> ordered = ((SortedRangeSet) valueSet).getOrderedRanges();
            List<Range> ranges = new ArrayList<>();
            for (com.amazonaws.athena.connector.lambda.domain.predicate.Range athenaRange : ordered) {
                ranges.add(toSleeperRange(field, athenaRange));
            }
            return ranges;
        } else if (valueSet instanceof EquatableValueSet) {
            EquatableValueSet equatable = (EquatableValueSet) valueSet;
            if (!equatable.isWhiteList()) {
                return null;
            }
            int count = equatable.getValues().getRowCount();
            List<Range> ranges = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                ranges.add(rangeFactory.createExactRange(field, toSleeperValue(field.getType(), equatable.getValue(i))));
            }
            return ranges;
        }
        return null;
    }

    // Translates a single row-key value set into one Sleeper range, or null if it cannot be represented as
    // a single contiguous range.
    private Range rowKeyRange(Field field, ValueSet valueSet) {
        if (valueSet instanceof SortedRangeSet) {
            List<com.amazonaws.athena.connector.lambda.domain.predicate.Range> ordered = ((SortedRangeSet) valueSet).getOrderedRanges();
            if (ordered.size() != 1) {
                return null;
            }
            return toSleeperRange(field, ordered.get(0));
        } else if (valueSet instanceof EquatableValueSet) {
            EquatableValueSet equatable = (EquatableValueSet) valueSet;
            if (!equatable.isWhiteList() || equatable.getValues().getRowCount() != 1) {
                return null;
            }
            return rangeFactory.createExactRange(field, toSleeperValue(field.getType(), equatable.getValue(0)));
        }
        return null;
    }

    private Range toSleeperRange(Field field, com.amazonaws.athena.connector.lambda.domain.predicate.Range athenaRange) {
        Type type = field.getType();
        Object min;
        boolean minInclusive;
        if (athenaRange.getLow().isLowerUnbounded()) {
            min = rangeFactory.createRangeCoveringAllValues(field).getMin();
            minInclusive = true;
        } else {
            min = toSleeperValue(type, athenaRange.getLow().getValue());
            minInclusive = athenaRange.getLow().getBound() == Marker.Bound.EXACTLY;
        }
        Object max;
        boolean maxInclusive;
        if (athenaRange.getHigh().isUpperUnbounded()) {
            max = null;
            maxInclusive = false;
        } else {
            max = toSleeperValue(type, athenaRange.getHigh().getValue());
            maxInclusive = athenaRange.getHigh().getBound() == Marker.Bound.EXACTLY;
        }
        return rangeFactory.createRange(field, min, minInclusive, max, maxInclusive);
    }

    private static Object toSleeperValue(Type type, Object value) {
        if (type instanceof StringType) {
            return value.toString();
        } else if (type instanceof ByteArrayType) {
            return (byte[]) value;
        } else if (type instanceof IntType) {
            return ((Number) value).intValue();
        } else if (type instanceof LongType) {
            return ((Number) value).longValue();
        } else {
            throw new IllegalArgumentException("Unexpected row key type: " + type);
        }
    }
}
