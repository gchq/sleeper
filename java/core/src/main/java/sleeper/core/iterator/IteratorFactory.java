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
package sleeper.core.iterator;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.model.DataEngine;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A factory class for Sleeper iterators.
 */
public class IteratorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(IteratorFactory.class);

    private final ObjectFactory inner;

    public IteratorFactory(ObjectFactory inner) {
        this.inner = inner;
    }

    /**
     * Creates an initialises an iterator.
     *
     * The named iterator class is created and its {@link ConfigStringIterator#init(String, Schema)} method is called.
     * If the special keyword for aggregation is specified as the class name
     * {@link DataEngine#AGGREGATION_ITERATOR_NAME},
     * then an aggregation iterator is created and initialised.
     *
     * @param  iteratorConfig            config object holding iterator details
     * @param  schema                    the Sleeper {@link Schema} of the {@link Row} objects
     * @return                           an initialised iterator
     * @throws IteratorCreationException if an iterator can't be created, for example it's class definition can't be
     *                                   found
     * @see                              ConfigStringAggregationFilteringIterator
     */
    public SortedRowIterator getIterator(IteratorConfig iteratorConfig, Schema schema) throws IteratorCreationException {
        try {
            if (iteratorConfig.getFilteringString() == null && iteratorConfig.getAggregationString() == null) {
                ConfigStringIterator iterator;
                String className = iteratorConfig.getIteratorClassName();

                // If aggregation keyword is used, create specific iterator
                if (className.equalsIgnoreCase(DataEngine.AGGREGATION_ITERATOR_NAME)) {
                    iterator = new ConfigStringAggregationFilteringIterator();
                } else {
                    iterator = inner.getObject(className, ConfigStringIterator.class);
                }
                LOGGER.debug("Created iterator of class {}", className);
                iterator.init(iteratorConfig.getIteratorConfigString(), schema);
                LOGGER.debug("Initialised iterator with config {}", iteratorConfig.getIteratorConfigString());
                return iterator;
            } else {
                AggregationFilteringIterator iterator = new AggregationFilteringIterator();
                iterator.setFilterAggregationConfig(getConfigFromProperties(iteratorConfig, schema));
                iterator.setSchema(schema);
                return iterator;
            }
        } catch (ObjectFactoryException exc) {
            throw new IteratorCreationException(exc);
        }
    }

    /**
     * Gets the filter aggregation config when set by table properties.
     * Currently just gets the filter ageoff(column,age).
     *
     *
     * @param  iteratorConfig config for an iterator that should have filters set in it
     * @param  schema         the Sleeper {@link Schema} of the {@link Row} objects
     * @return                filter aggregation config to be used in an iterator
     */
    private FilterAggregationConfig getConfigFromProperties(IteratorConfig iteratorConfig, Schema schema) {
        List<String> groupingColumns = new ArrayList<>(schema.getRowKeyFieldNames());
        long maxAge = 0L;
        String filterName = null;

        if (iteratorConfig.getFilteringString() != null && !iteratorConfig.getFilteringString().equals("")) {
            String[] filterParts = iteratorConfig.getFilteringString().split("\\(");
            if ("ageoff".equals(filterParts[0].toLowerCase(Locale.ENGLISH))) {
                String[] filterInput = StringUtils.chop(filterParts[1]).split(","); //Chop to remove the trailing ')'
                filterName = filterInput[0];
                maxAge = Long.parseLong(filterInput[1]);
                groupingColumns.add(filterInput[0]);
            } else {
                throw new IllegalStateException("Sleeper table filter not set to match ageOff(column,age), was: " + filterParts[0]);
            }
        }
        List<Aggregation> aggregations = List.of();
        if (iteratorConfig.getAggregationString() != null) {
            aggregations = generateAggregationsFromProperty(iteratorConfig);
            validateAggregations(aggregations, schema);
        }

        return new FilterAggregationConfig(groupingColumns,
                filterName != null ? Optional.of(filterName) : Optional.empty(),
                maxAge,
                aggregations);
    }

    private List<Aggregation> generateAggregationsFromProperty(IteratorConfig iteratorConfig) {
        List<Aggregation> aggregations = new ArrayList<Aggregation>();
        String[] aggregationSplits = iteratorConfig.getAggregationString().split("\\),");
        Arrays.stream(aggregationSplits).forEach(presentAggregation -> {
            AggregationString aggObject = new AggregationString(presentAggregation
                    .replaceAll("\\)", "")
                    .split("\\("));

            try {
                aggregations.add(new Aggregation(aggObject.getColumnName(),
                        AggregationOp.valueOf(aggObject.getOpName().toUpperCase(Locale.ENGLISH).replaceFirst("^MAP_", ""))));
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("Unable to parse operand. Operand: " + aggObject.getOpName());
            }
        });
        return aggregations;
    }

    private void validateAggregations(List<Aggregation> aggregations, Schema schema) {
        validateNoRowKeySortKeyAggregations(aggregations, schema);
        validateNoDuplicateAggregations(aggregations);
        validateAllColumnsHaveAggregations(aggregations, schema);
    }

    private void validateNoRowKeySortKeyAggregations(List<Aggregation> aggregations, Schema schema) {
        List<Aggregation> rowKeySortKeyViolations = aggregations.stream().filter(agg -> {
            return schema.getRowKeyFieldNames().contains(agg.column()) ||
                    schema.getSortKeyFieldNames().contains(agg.column());
        }).toList();

        if (!rowKeySortKeyViolations.isEmpty()) {
            String errStr = rowKeySortKeyViolations.stream()
                    .map(Aggregation::column)
                    .collect(Collectors.joining(", "));
            throw new IllegalStateException("Column for aggregation not allowed to be a Row Key or Sort Key. Column names: " + errStr);
        }
    }

    private void validateNoDuplicateAggregations(List<Aggregation> aggregations) {
        HashMap<String, Boolean> aggMap = new HashMap<String, Boolean>();
        aggregations.stream().forEach(aggregation -> {
            if (aggMap.putIfAbsent(aggregation.column(), Boolean.TRUE) != null) {
                throw new IllegalStateException("Not allowed duplicate columns for aggregation. Column name: " + aggregation.column());
            }
        });
    }

    private void validateAllColumnsHaveAggregations(List<Aggregation> aggregations, Schema schema) {
        List<String> aggregationColumns = new ArrayList<String>();
        aggregations.stream().forEach(aggregation -> {
            aggregationColumns.add(aggregation.column());
        });

        if (!aggregationColumns.containsAll(schema.getValueFieldNames())) {
            String errStr = schema.getValueFieldNames().stream()
                    .filter(presCol -> !aggregationColumns.contains(presCol))
                    .collect(Collectors.joining(", "));
            throw new IllegalStateException("Not all value fields have aggregation declared. Missing columns: " + errStr);
        }
    }

    /**
     * Record to provide parse object from the aggregation string for clarity.
     *
     * @param aggregationParts array contain sepeate parts of the aggregation.
     */
    private record AggregationString(String[] aggregationParts) {

        String getOpName() {
            return aggregationParts[0];
        }

        String getColumnName() {
            return aggregationParts[1];
        }
    }

}
