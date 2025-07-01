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
package sleeper.athena;

import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.common.collect.ImmutableMap;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;

import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * FilterTranslator acts as a go-between for Athena ValueSets and Parquet Filters.
 */
public class FilterTranslator {
    private final Map<String, Type> schemaTypes;

    public FilterTranslator(Schema schema) {
        Map<String, Type> types = new HashMap<>();
        schema.getAllFields().forEach(field -> types.put(field.getName(), field.getType()));
        schemaTypes = ImmutableMap.copyOf(types);
    }

    /**
     * Creates a single Parquet FilterPredicate based on the ValueSets. Will only Filter based on Primitive types and
     * will therefore ignore all others. If there is no resulting predicate (either caused by an empty, or null map or a
     * map of non-primitive fields), this method will return null. If more than one primitive ValueSet exists in the
     * map, the predicates of all the ValueSets will be ANDed together.
     *
     * @param  valueSets a map of field name to ValueSet
     * @return           a single parquet filter based on the ValueSets
     */
    public FilterPredicate toPredicate(Map<String, ValueSet> valueSets) {
        if (valueSets == null || valueSets.isEmpty()) {
            return null;
        }

        long primitivePredicates = valueSets.keySet().stream()
                .map(schemaTypes::get)
                .filter(type -> type instanceof PrimitiveType)
                .count();

        if (primitivePredicates == 0) {
            return null;
        }

        FilterPredicate filter = null;
        for (Map.Entry<String, ValueSet> entry : valueSets.entrySet()) {
            filter = and(filter, createPredicate(entry.getKey(), entry.getValue()));
        }

        return filter;
    }

    private FilterPredicate createPredicate(String columnName, ValueSet acceptedValues) {
        if (acceptedValues instanceof SortedRangeSet) {
            return createRangePredicate(columnName, (SortedRangeSet) acceptedValues);
        } else if (acceptedValues instanceof EquatableValueSet) {
            return createExactValuePredicate(columnName, (EquatableValueSet) acceptedValues);
        }

        return null;
    }

    private FilterPredicate createExactValuePredicate(String columnName, EquatableValueSet acceptedValues) {
        Type type = schemaTypes.get(columnName);
        List<FilterPredicate> filters;
        if (type instanceof StringType) {
            filters = createExactPredicates(columnName, acceptedValues,
                    FilterApi::binaryColumn,
                    o -> Binary.fromString(o.toString()));

        } else if (type instanceof ByteArrayType) {
            filters = createExactPredicates(columnName, acceptedValues,
                    FilterApi::binaryColumn,
                    bytes -> Binary.fromConstantByteArray((byte[]) bytes));

        } else if (type instanceof IntType) {
            filters = createExactPredicates(columnName, acceptedValues,
                    FilterApi::intColumn);

        } else if (type instanceof LongType) {
            filters = createExactPredicates(columnName, acceptedValues,
                    FilterApi::longColumn);
        } else {
            return null;
        }

        Optional<FilterPredicate> reduce = filters.stream().filter(Objects::nonNull).reduce(FilterTranslator::or);

        if (!reduce.isPresent()) {
            return null;
        }

        FilterPredicate filterPredicate = reduce.get();
        if (acceptedValues.isWhiteList()) {
            return filterPredicate;
        }

        return FilterApi.not(filterPredicate);

    }

    private <CT extends Comparable<CT>, C extends Operators.Column<CT> & Operators.SupportsEqNotEq> List<FilterPredicate> createExactPredicates(
            String columnName,
            EquatableValueSet acceptedValues,
            Function<String, C> columnCreator) {
        return createExactPredicates(columnName, acceptedValues, columnCreator, o -> (CT) o);
    }

    private <CT extends Comparable<CT>, C extends Operators.Column<CT> & Operators.SupportsEqNotEq> List<FilterPredicate> createExactPredicates(
            String columnName,
            EquatableValueSet acceptedValues,
            Function<String, C> columnCreator,
            Function<Object, CT> valueTransformer) {
        int rowCount = acceptedValues.getValues().getRowCount();
        List<FilterPredicate> filterPredicate = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            filterPredicate.add(FilterApi.eq(columnCreator.apply(columnName), valueTransformer.apply(acceptedValues.getValue(i))));
        }
        return filterPredicate;
    }

    private FilterPredicate createRangePredicate(String columnName, SortedRangeSet acceptedValues) {
        Type type = schemaTypes.get(columnName);
        Stream<FilterPredicate> filters;
        if (type instanceof StringType) {
            filters = createRangePredicates(columnName, acceptedValues,
                    FilterApi::binaryColumn,
                    o -> Binary.fromString(o.toString()));

        } else if (type instanceof ByteArrayType) {
            filters = createRangePredicates(columnName, acceptedValues,
                    FilterApi::binaryColumn,
                    o -> Binary.fromConstantByteArray((byte[]) o));

        } else if (type instanceof IntType) {
            filters = createRangePredicates(columnName, acceptedValues,
                    FilterApi::intColumn);

        } else if (type instanceof LongType) {
            filters = createRangePredicates(columnName, acceptedValues,
                    FilterApi::longColumn);
        } else {
            return null;
        }

        return filters.reduce(FilterTranslator::or).orElse(null);

    }

    private <CT extends Comparable<CT>, R, C extends Operators.Column<CT> & Operators.SupportsLtGt> Stream<FilterPredicate> createRangePredicates(
            String columnName,
            SortedRangeSet valueSet,
            Function<String, C> columnCreator) {
        return createRangePredicates(columnName, valueSet, columnCreator, o -> (CT) o);

    }

    private <CT extends Comparable<CT>, C extends Operators.Column<CT> & Operators.SupportsLtGt> Stream<FilterPredicate> createRangePredicates(
            String columnName,
            SortedRangeSet valueSet,
            Function<String, C> columnCreator,
            Function<Object, CT> valueTransformer) {
        return valueSet.getOrderedRanges()
                .stream().map(range -> {
                    FilterPredicate lowPredicate = null;
                    if (!range.getLow().isLowerUnbounded()) {
                        if (range.getLow().getBound().equals(Marker.Bound.EXACTLY)) {
                            lowPredicate = FilterApi.gtEq(columnCreator.apply(columnName),
                                    valueTransformer.apply(range.getLow().getValue()));
                        } else {
                            lowPredicate = FilterApi.gt(columnCreator.apply(columnName),
                                    valueTransformer.apply(range.getLow().getValue()));
                        }
                    }
                    if (!range.getHigh().isUpperUnbounded()) {
                        if (range.getHigh().getBound().equals(Marker.Bound.EXACTLY)) {
                            return and(lowPredicate, FilterApi.ltEq(columnCreator.apply(columnName),
                                    valueTransformer.apply(range.getHigh().getValue())));
                        } else {
                            return and(lowPredicate, FilterApi.lt(columnCreator.apply(columnName),
                                    valueTransformer.apply(range.getHigh().getValue())));
                        }
                    }
                    return lowPredicate;
                });
    }

    /**
     * Wrapper around the FilterApi.and method which allows either side to be null. If both sides are null, null is
     * returned as the result. If one side is null, the other predicate will be returned. If both sides are non-null,
     * the predicates will be and-ed together
     *
     * @param  lhs left predicate
     * @param  rhs right predicate
     * @return     a Predicate which ensures both left and right predicates pass.
     */
    public static FilterPredicate and(FilterPredicate lhs, FilterPredicate rhs) {
        return merge(lhs, rhs, FilterApi::and);
    }

    /**
     * Wrapper around the FilterApi.or method which allows either side to be null. If both sides are null, null is
     * returned as the result. If one side is null, the other predicate will be returned. If both sides are non-null,
     * the predicates will be or-ed together
     *
     * @param  lhs left predicate
     * @param  rhs right predicate
     * @return     a Predicate which ensures both left and right predicates pass.
     */
    public static FilterPredicate or(FilterPredicate lhs, FilterPredicate rhs) {
        return merge(lhs, rhs, FilterApi::or);
    }

    private static FilterPredicate merge(FilterPredicate lhs, FilterPredicate rhs, BinaryOperator<FilterPredicate> mergeFunction) {
        FilterPredicate merged = lhs;
        if (merged == null) {
            merged = rhs;
        } else if (rhs != null) {
            merged = mergeFunction.apply(lhs, rhs);
        }

        return merged;
    }
}
