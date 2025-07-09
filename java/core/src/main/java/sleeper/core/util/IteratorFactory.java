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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.AggregationFilteringIterator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.iterator.SortedRowIterator;
import sleeper.core.properties.model.CompactionMethod;
import sleeper.core.schema.Schema;

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
     * The named iterator class is created and its {@link SortedRecordIterator#init(String, Schema)} method is called.
     * If the special keyword for aggregation is specified as the class name
     * {@link CompactionMethod#AGGREGATION_ITERATOR_NAME},
     * then an aggregation iterator is created and initialised.
     *
     * @param  iteratorClassName         the fully qualified iterator class name or aggregation keyword
     * @param  iteratorConfig            the iterator configuration string
     * @param  schema                    the schema the iterator should be configured for
     * @return                           an initialised iterator
     * @throws IteratorCreationException if an iterator can't be created, for example it's class definition can't be
     *                                   found
     * @see                              AggregationFilteringIterator
     */
    public SortedRowIterator getIterator(String iteratorClassName, String iteratorConfig, Schema schema) throws IteratorCreationException {
        try {
            SortedRowIterator iterator;

            // If aggregation keyword is used, create specific iterator
            if (iteratorClassName.equalsIgnoreCase(CompactionMethod.AGGREGATION_ITERATOR_NAME)) {
                iterator = new AggregationFilteringIterator();
            } else {
                iterator = inner.getObject(iteratorClassName, SortedRowIterator.class);
            }
            LOGGER.debug("Created iterator of class {}", iteratorClassName);
            iterator.init(iteratorConfig, schema);
            LOGGER.debug("Initialised iterator with config {}", iteratorConfig);
            return iterator;
        } catch (ObjectFactoryException exc) {
            throw new IteratorCreationException(exc);
        }
    }
}
