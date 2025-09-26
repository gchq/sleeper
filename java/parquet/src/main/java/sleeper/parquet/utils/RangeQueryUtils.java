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
package sleeper.parquet.utils;

import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;

import sleeper.core.partition.Partition;
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
import static org.apache.parquet.filter2.predicate.FilterApi.or;

public class RangeQueryUtils {

    private RangeQueryUtils() {
    }

    public static FilterPredicate getFilterPredicateMultidimensionalKey(
            List<Region> regions,
            Region partitionRegion) {
        FilterPredicate anyRegionFilter = getFilterPredicate(regions);

        // Add in restriction that only want data from the partition (partitions do not include the maximum value)
        FilterPredicate partitionPredicate = getFilterPredicateNoCanonicalise(partitionRegion);
        return and(partitionPredicate, anyRegionFilter);
    }

    public static FilterPredicate getFilterPredicate(Partition partition) {
        return getFilterPredicateNoCanonicalise(partition.getRegion());
    }

    public static FilterPredicate getFilterPredicate(Region region) {
        return getFilterPredicateNoCanonicalise(region);
    }

    private static FilterPredicate getFilterPredicate(List<Region> regions) {
        List<FilterPredicate> filters = new ArrayList<>();
        for (Region region : regions) {
            Region canonicalRegion = RegionCanonicaliser.canonicaliseRegion(region);
            filters.add(getFilterPredicateNoCanonicalise(canonicalRegion));
        }
        return orList(filters);
    }

    private static FilterPredicate orList(List<FilterPredicate> predicates) {
        FilterPredicate anyPredicate = null;
        for (FilterPredicate predicate : predicates) {
            if (null == anyPredicate) {
                anyPredicate = predicate;
            } else {
                anyPredicate = or(anyPredicate, predicate);
            }
        }
        return anyPredicate;
    }

    private static FilterPredicate getFilterPredicateNoCanonicalise(Region region) {
        FilterPredicate fieldsFilter = null;
        for (Range range : region.getRangesUnordered()) {
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
        String keyName = range.getFieldName();
        Type keyType = range.getFieldType();
        if (keyType instanceof IntType) {
            return getFilterPredicateForKey(intColumn(keyName), (Integer) range.getMin(), (Integer) range.getMax());
        }
        if (keyType instanceof LongType) {
            return getFilterPredicateForKey(longColumn(keyName), (Long) range.getMin(), (Long) range.getMax());
        }
        if (keyType instanceof StringType) {
            return getFilterPredicateForKey(binaryColumn(keyName),
                    wrapString((String) range.getMin()),
                    wrapString((String) range.getMax()));
        }
        if (keyType instanceof ByteArrayType) {
            return getFilterPredicateForKey(binaryColumn(keyName),
                    wrapByteArray((byte[]) range.getMin()),
                    wrapByteArray((byte[]) range.getMax()));
        }
        throw new IllegalArgumentException("Unknown type " + keyType);
    }

    private static <T extends Comparable<T>, C extends Operators.Column<T> & Operators.SupportsLtGt> FilterPredicate getFilterPredicateForKey(C column, T minKey, T maxKey) {
        if (null == minKey) {
            throw new IllegalArgumentException("The minimum range key cannot be null");
        }

        FilterPredicate greaterThanOrEqRangeMin = gtEq(column, minKey);
        FilterPredicate lessThanRangeMax = null;
        if (null != maxKey) {
            lessThanRangeMax = lt(column, maxKey);
        }
        return null == lessThanRangeMax ? greaterThanOrEqRangeMin : and(greaterThanOrEqRangeMin, lessThanRangeMax);
    }

    private static Binary wrapString(String value) {
        if (value == null) {
            return null;
        } else {
            return Binary.fromString(value);
        }
    }

    private static Binary wrapByteArray(byte[] value) {
        if (value == null) {
            return null;
        } else {
            return Binary.fromConstantByteArray(value);
        }
    }
}
