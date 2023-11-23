/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.io.parquet.utils;

import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Operators.LongColumn;
import org.apache.parquet.io.api.Binary;

import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.range.RegionCanonicaliser;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;

import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;

public class RangeQueryUtils {

    private RangeQueryUtils() {
    }

    public static FilterPredicate getFilterPredicateMultidimensionalKey(
            List<Region> regions,
            Region partitionRegion) {
        FilterPredicate anyRegionFilter = getFilterPredicate(regions);

        // Add in restriction that only want data from the partition (partitions do not include the maximum value)
        FilterPredicate partitionPredicate = getFilterPredicateNoCanonicalise(partitionRegion);
        return org.apache.parquet.filter2.predicate.FilterApi.and(partitionPredicate, anyRegionFilter);
    }

    private static FilterPredicate getFilterPredicate(List<Region> regions) {
        List<FilterPredicate> filters = new ArrayList<>();
        for (Region region : regions) {
            Region canonicalRegion = RegionCanonicaliser.canonicaliseRegion(region);
            filters.add(getFilterPredicateNoCanonicalise(canonicalRegion));
        }
        return or(filters);
    }

    private static FilterPredicate or(List<FilterPredicate> predicates) {
        FilterPredicate anyPredicate = null;
        for (FilterPredicate predicate : predicates) {
            if (null == anyPredicate) {
                anyPredicate = predicate;
            } else {
                anyPredicate = org.apache.parquet.filter2.predicate.FilterApi.or(anyPredicate, predicate);
            }
        }
        return anyPredicate;
    }

    private static FilterPredicate getFilterPredicateNoCanonicalise(Region region) {
        FilterPredicate fieldsFilter = null;
        for (Range range : region.getRanges()) {
            FilterPredicate predicateForThisDimension = getFilterPredicate(range);
            if (null == fieldsFilter) {
                fieldsFilter = predicateForThisDimension;
            } else {
                fieldsFilter = and(fieldsFilter, predicateForThisDimension);
            }
        }
        return fieldsFilter;
    }

    private static FilterPredicate getFilterPredicate(Range range) {
        Type keyType = range.getFieldType();
        if (keyType instanceof IntType) {
            return getFilterPredicateForIntKey(range.getFieldName(), (Integer) range.getMin(), (Integer) range.getMax());
        }
        if (keyType instanceof LongType) {
            return getFilterPredicateForLongKey(range.getFieldName(), (Long) range.getMin(), (Long) range.getMax());
        }
        if (keyType instanceof StringType) {
            return getFilterPredicateForStringKey(range.getFieldName(), (String) range.getMin(), (String) range.getMax());
        }
        if (keyType instanceof ByteArrayType) {
            return getFilterPredicateForByteArrayKey(range.getFieldName(), (byte[]) range.getMin(), (byte[]) range.getMax());
        }
        throw new IllegalArgumentException("Unknown type " + keyType);
    }

    // TODO Code duplication in the following methods.
    private static FilterPredicate getFilterPredicateForIntKey(
            String keyName, Integer minKey, Integer maxKey) {
        if (null == minKey) {
            throw new IllegalArgumentException("The minimum range key cannot be null");
        }

        IntColumn intColumn = intColumn(keyName);
        FilterPredicate greaterThanOrEqRangeMin = gtEq(intColumn, minKey);
        FilterPredicate lessThanRangeMax = null;
        if (null != maxKey) {
            lessThanRangeMax = lt(intColumn, maxKey);
        }
        return null == lessThanRangeMax ? greaterThanOrEqRangeMin : and(greaterThanOrEqRangeMin, lessThanRangeMax);
    }

    private static FilterPredicate getFilterPredicateForLongKey(
            String keyName, Long minKey, Long maxKey) {
        if (null == minKey) {
            throw new IllegalArgumentException("The minimum range key cannot be null");
        }

        LongColumn longColumn = longColumn(keyName);
        FilterPredicate greaterThanOrEqRangeMin = gtEq(longColumn, minKey);
        FilterPredicate lessThanRangeMax = null;
        if (null != maxKey) {
            lessThanRangeMax = lt(longColumn, maxKey);
        }
        return null == lessThanRangeMax ? greaterThanOrEqRangeMin : and(greaterThanOrEqRangeMin, lessThanRangeMax);
    }

    private static FilterPredicate getFilterPredicateForStringKey(
            String keyName, String minKey, String maxKey) {
        if (null == minKey) {
            throw new IllegalArgumentException("The minimum range key cannot be null");
        }

        BinaryColumn binaryColumn = binaryColumn(keyName);
        FilterPredicate greaterThanOrEqRangeMin = gtEq(binaryColumn, Binary.fromString(minKey));
        FilterPredicate lessThanRangeMax = null;
        if (null != maxKey) {
            lessThanRangeMax = lt(binaryColumn, Binary.fromString(maxKey));
        }

        return null == lessThanRangeMax ? greaterThanOrEqRangeMin : and(greaterThanOrEqRangeMin, lessThanRangeMax);
    }

    private static FilterPredicate getFilterPredicateForByteArrayKey(
            String keyName, byte[] minKey, byte[] maxKey) {
        if (null == minKey) {
            throw new IllegalArgumentException("The minimum range key cannot be null");
        }

        BinaryColumn binaryColumn = binaryColumn(keyName);
        FilterPredicate greaterThanOrEqRangeMin = gtEq(binaryColumn(keyName), Binary.fromConstantByteArray(minKey));
        FilterPredicate lessThanRangeMax = null;
        if (null != maxKey) {
            lessThanRangeMax = lt(binaryColumn, Binary.fromConstantByteArray((maxKey)));
        }

        return null == lessThanRangeMax ? greaterThanOrEqRangeMin : and(greaterThanOrEqRangeMin, lessThanRangeMax);
    }
}
