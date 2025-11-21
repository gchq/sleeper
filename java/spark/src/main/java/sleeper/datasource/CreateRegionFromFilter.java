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
package sleeper.datasource;

import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Or;

import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.PrimitiveType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class CreateRegionFromFilter {

    public static Optional<List<Region>> createRegionsFromFilter(Filter filter, Schema schema) {
        if (filter instanceof Or || filter instanceof In) {
            if (filter instanceof Or) {
                Or or = (Or) filter;
                Filter left = or.left();
                Filter right = or.right();
                Optional<Region> leftRegion = createRegionFromFilter(left, schema);
                Optional<Region> rightRegion = createRegionFromFilter(right, schema);
                if (leftRegion.isEmpty() || rightRegion.isEmpty()) {
                    return Optional.empty();
                } else {
                    return Optional.of(List.of(leftRegion.get(), rightRegion.get()));
                }
            } else {
                In in = (In) filter;
                List<Region> regions = new ArrayList<>();
                Object[] values = in.values();
                for (Object value : values) {
                    Filter equalFilter = new EqualTo(in.attribute(), value);
                    Optional<Region> optional = createRegionFromFilter(equalFilter, schema);
                    if (optional.isPresent()) {
                        regions.add(optional.get());
                    }
                }
                if (regions.isEmpty()) {
                    return Optional.empty();
                } else {
                    return Optional.of(regions);
                }
            }
        } else {
            Optional<Region> optionalRegion = createRegionFromFilter(filter, schema);
            if (optionalRegion.isEmpty()) {
                return Optional.empty();
            } else {
                return Optional.of(List.of(optionalRegion.get()));
            }
        }
    }

    public static Optional<Region> createRegionFromFilter(Filter filter, Schema schema) {
        MutableRegion mutableRegion = new MutableRegion(schema);
        updateRegionWithFilter(mutableRegion, filter, new HashSet<>(schema.getRowKeyFieldNames()));
        Region region = mutableRegion.getRegion();
        if (region.equals(Region.coveringAllValuesOfAllRowKeys(schema))) {
            System.out.println("Created empty optional region for filter " + filter);
            return Optional.empty();
        }
        System.out.println("Entire region: " + Region.coveringAllValuesOfAllRowKeys(schema));
        System.out.println("Created region " + region + " for filter " + filter);
        return Optional.of(region);
    }

    private static class MutableRegion {
        private final Map<String, MutableRange> rowKeyToMutableRange;

        MutableRegion(Schema schema) {
            this.rowKeyToMutableRange = new HashMap<>();
            RangeFactory rangeFactory = new RangeFactory(schema);
            for (Field rowKeyField : schema.getRowKeyFields()) {
                this.rowKeyToMutableRange.put(rowKeyField.getName(), new MutableRange(rowKeyField, rangeFactory));
            }
        }

        MutableRange getMutableRange(String rowKeyFieldName) {
            return rowKeyToMutableRange.get(rowKeyFieldName);
        }

        Region getRegion() {
            return new Region(rowKeyToMutableRange.values().stream().map(mr -> mr.getRange()).collect(Collectors.toList()));
        }
    }

    private static class MutableRange {
        private final String fieldName;
        private final RangeFactory rangeFactory;
        Object min;
        boolean minInclusive = true;
        Object max = null;
        boolean maxInclusive = false;

        MutableRange(Field field, RangeFactory rangeFactory) {
            this.fieldName = field.getName();
            this.rangeFactory = rangeFactory;
            this.min = PrimitiveType.getMinimum(field.getType());
        }

        Range getRange() {
            return rangeFactory.createRange(fieldName, min, minInclusive, max, maxInclusive);
        }
    }

    private static void updateRegionWithFilter(MutableRegion mutableRegion, Filter filter, Set<String> rowKeyFieldNames) {
        if (filter instanceof EqualTo) {
            String fieldName = ((EqualTo) filter).attribute();
            if (!rowKeyFieldNames.contains(fieldName)) {
                return;
            }
            Object wantedKey = ((EqualTo) filter).value();
            MutableRange mutableRange = mutableRegion.getMutableRange(fieldName);
            mutableRange.minInclusive = true;
            mutableRange.min = wantedKey;
            mutableRange.maxInclusive = true;
            mutableRange.max = wantedKey;
        } else if (filter instanceof GreaterThan) {
            String fieldName = ((GreaterThan) filter).attribute();
            if (!rowKeyFieldNames.contains(fieldName)) {
                return;
            }
            Object minimumKey = ((GreaterThan) filter).value();
            MutableRange mutableRange = mutableRegion.getMutableRange(fieldName);
            mutableRange.minInclusive = false;
            mutableRange.min = minimumKey;
        } else if (filter instanceof GreaterThanOrEqual) {
            String fieldName = ((GreaterThanOrEqual) filter).attribute();
            if (!rowKeyFieldNames.contains(fieldName)) {
                return;
            }
            Object minimumKey = ((GreaterThanOrEqual) filter).value();
            MutableRange mutableRange = mutableRegion.getMutableRange(fieldName);
            mutableRange.minInclusive = true;
            mutableRange.min = minimumKey;
        } else if (filter instanceof LessThan) {
            String fieldName = ((LessThan) filter).attribute();
            if (!rowKeyFieldNames.contains(fieldName)) {
                return;
            }
            Object maxiumKey = ((LessThan) filter).value();
            MutableRange mutableRange = mutableRegion.getMutableRange(fieldName);
            mutableRange.maxInclusive = false;
            mutableRange.max = maxiumKey;
        } else if (filter instanceof LessThanOrEqual) {
            String fieldName = ((LessThan) filter).attribute();
            if (!rowKeyFieldNames.contains(fieldName)) {
                return;
            }
            Object maxiumKey = ((LessThanOrEqual) filter).value();
            MutableRange mutableRange = mutableRegion.getMutableRange(fieldName);
            mutableRange.maxInclusive = true;
            mutableRange.max = maxiumKey;
        }
    }
}
