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
import sleeper.core.iterator.ConfigStringIterator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.schema.Schema;

/**
 * A factory class for Sleeper iterators.
 */
public class IteratorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(IteratorFactory.class);

    private final ObjectFactory inner;
    private final String iteratorClassName;
    private final String iteratorConfig;
    private final Schema schema;

    public IteratorFactory(Builder builder) {
        inner = builder.inner;
        this.iteratorClassName = builder.iteratorClassName;
        this.iteratorConfig = builder.iteratorConfig;
        this.schema = builder.schema;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates an initialises an iterator.
     *
     * The named iterator class is created and its {@link ConfigStringIterator#init(String, Schema)} method is called.
     * If the special keyword for aggregation is specified as the class name
     * {@link DataEngine#AGGREGATION_ITERATOR_NAME},
     * then an aggregation iterator is created and initialised.
     *
     * @return                           an initialised iterator
     * @throws IteratorCreationException if an iterator can't be created, for example it's class definition can't be
     *                                   found
     * @see                              AggregationFilteringIterator
     */
    public ConfigStringIterator getIterator() throws IteratorCreationException {
        try {
            ConfigStringIterator iterator;

            // If aggregation keyword is used, create specific iterator
            if (iteratorClassName.equalsIgnoreCase(DataEngine.AGGREGATION_ITERATOR_NAME)) {
                iterator = new AggregationFilteringIterator();
            } else {
                iterator = inner.getObject(iteratorClassName, ConfigStringIterator.class);
            }
            LOGGER.debug("Created iterator of class {}", iteratorClassName);
            iterator.init(iteratorConfig, schema);
            LOGGER.debug("Initialised iterator with config {}", iteratorConfig);
            return iterator;
        } catch (ObjectFactoryException exc) {
            throw new IteratorCreationException(exc);
        }
    }

    /**
     * Builder for iterator factory object.
     */
    public static final class Builder {
        private ObjectFactory inner;
        private String iteratorClassName;
        private String iteratorConfig;
        private Schema schema;

        private Builder() {
        }

        /**
         * Sets the object factory to be used for certain iterators.
         *
         * @param  inner the object factory to be used
         * @return       builder for method chaining
         */
        public Builder inner(ObjectFactory inner) {
            this.inner = inner;
            return this;
        }

        /**
         * Sets the iterator class name.
         *
         * @param  iteratorClassName the name of the iterator class to build
         * @return                   builder for method chaining
         */
        public Builder iteratorClassName(String iteratorClassName) {
            this.iteratorClassName = iteratorClassName;
            return this;
        }

        /**
         * Sets the iterator config.
         *
         * @param  iteratorConfig the config string to be used for the iterator
         * @return                builder for method chaining
         */
        public Builder iteratorConfig(String iteratorConfig) {
            this.iteratorConfig = iteratorConfig;
            return this;
        }

        /**
         * Sets the schema for the iterator to use.
         *
         * @param  schema the schema for the iterator to use
         * @return        builder for method chaining
         */
        public Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public IteratorFactory build() {
            return new IteratorFactory(this);
        }
    }
}
