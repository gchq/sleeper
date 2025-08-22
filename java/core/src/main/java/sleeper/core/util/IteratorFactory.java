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
package sleeper.core.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.AggregationFilteringIterator;
import sleeper.core.iterator.ConfigStringAggregationFilteringIterator;
import sleeper.core.iterator.ConfigStringIterator;
import sleeper.core.iterator.FilterAggregationConfig;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.iterator.SortedRowIterator;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.schema.Schema;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

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
     * @return                           an initialised iterator
     * @throws IteratorCreationException if an iterator can't be created, for example it's class definition can't be
     *                                   found
     * @see                              ConfigStringAggregationFilteringIterator
     */
    public SortedRowIterator getIterator(IteratorConfig iteratorConfig) throws IteratorCreationException {
        try {
            if (iteratorConfig.getFilters() == null) {
                ConfigStringIterator iterator;
                String className = iteratorConfig.getIteratorClassName();

                // If aggregation keyword is used, create specific iterator
                if (className.equalsIgnoreCase(DataEngine.AGGREGATION_ITERATOR_NAME)) {
                    iterator = new ConfigStringAggregationFilteringIterator();
                } else {
                    iterator = inner.getObject(className, ConfigStringIterator.class);
                }
                LOGGER.debug("Created iterator of class {}", className);
                iterator.init(iteratorConfig.getIteratorConfigString(), iteratorConfig.getSchema());
                LOGGER.debug("Initialised iterator with config {}", iteratorConfig.getIteratorConfigString());
                return iterator;
            } else {
                AggregationFilteringIterator iterator = new AggregationFilteringIterator();
                iterator.setFilterAggregationConfig(getConfigFromProperties(iteratorConfig));
                iterator.setSchema(iteratorConfig.getSchema());
                return iterator;
            }
        } catch (ObjectFactoryException exc) {
            throw new IteratorCreationException(exc);
        }
    }

    /**
     * Gets the filter aggregation config when set by table properties.
     * Currently just gets the filter ageoff(column,age).
     * Currently only uses filters from input but eventually will build aggregations from it too.
     *
     * @param  iteratorConfig config for an iterator that should have filters set in it
     * @return                filter aggregation config to be used in an iterator
     */
    private FilterAggregationConfig getConfigFromProperties(IteratorConfig iteratorConfig) {
        String[] filterParts = iteratorConfig.getFilters().split("\\(");
        if ("ageoff".equals(filterParts[0].toLowerCase(Locale.ENGLISH))) {
            String[] filterInput = StringUtils.chop(filterParts[1]).split(","); //Remove the trailing ')'
            Optional<String> filterColumn = Optional.of(filterInput[0]);
            long maxAge = Long.parseLong(filterInput[1]);
            return new FilterAggregationConfig(List.of(), filterColumn, maxAge, List.of());
        } else {
            throw new IllegalStateException("Sleeper table filter not set to match ageOff(column,age), was: " + filterParts[0]);
        }
    }
}
