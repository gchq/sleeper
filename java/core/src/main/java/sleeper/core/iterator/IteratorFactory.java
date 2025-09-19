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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;

import java.util.ArrayList;
import java.util.List;

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
     * Creates iterators based on the given configuration. This includes filtering, aggregation, and custom iterators.
     * For custom iterators, the named iterator class is created and its
     * {@link ConfigStringIterator#init(String, Schema)} method is called.
     *
     * @param  iteratorConfig            config object holding iterator details
     * @param  schema                    the Sleeper {@link Schema} of the {@link Row} objects
     * @return                           an initialised iterator
     * @throws IteratorCreationException if an iterator can't be created, for example it's class definition can't be
     *                                   found
     */
    public SortedRowIterator getIterator(IteratorConfig iteratorConfig, Schema schema) throws IteratorCreationException {
        try {
            List<SortedRowIterator> iterators = new ArrayList<>();
            iteratorConfig.getFilters().forEach(filter -> iterators.add(new AgeOffIterator(schema, filter)));
            if (!iteratorConfig.getAggregations().isEmpty()) {
                iterators.add(new AggregationIterator(schema, iteratorConfig.getAggregations()));
            }
            if (iteratorConfig.getIteratorClassName() != null) {
                ConfigStringIterator iterator;
                String className = iteratorConfig.getIteratorClassName();
                iterator = inner.getObject(className, ConfigStringIterator.class);

                LOGGER.debug("Created iterator of class {}", className);
                iterator.init(iteratorConfig.getIteratorConfigString(), schema);
                LOGGER.debug("Initialised iterator with config {}", iteratorConfig.getIteratorConfigString());
                iterators.add(iterator);
            }
            return new SortedRowIterators(iterators);
        } catch (RuntimeException | ObjectFactoryException exc) {
            throw new IteratorCreationException(exc);
        }
    }

}
