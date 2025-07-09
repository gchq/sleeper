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
package sleeper.ingest.runner.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.iterator.SortedRowIterator;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Iterates over a source of records with a Sleeper iterator applied to it. If the Sleeper iterator requires the records
 * to be in a specific order then the source iterator must supply them in that order.
 */
class RecordIteratorWithSleeperIteratorApplied implements CloseableIterator<Row> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordIteratorWithSleeperIteratorApplied.class);

    private final CloseableIterator<Row> inputIterator;
    private final CloseableIterator<Row> outputIterator;

    /**
     * Create an instance.
     *
     * @param  objectFactory             the {@link ObjectFactory} to use to create the Sleeper iterator
     * @param  sleeperSchema             the Sleeper {@link Schema} of the {@link Row} objects
     * @param  sleeperIteratorClassName  the Sleeper iterator to apply
     * @param  sleeperIteratorConfig     the configuration for the Sleeper iterator
     * @param  sourceIterator            the {@link CloseableIterator} to provide the source {@link Row} objects
     * @throws IteratorCreationException if there was a failure creating the Sleeper iterator
     */
    RecordIteratorWithSleeperIteratorApplied(
            ObjectFactory objectFactory,
            Schema sleeperSchema,
            String sleeperIteratorClassName,
            String sleeperIteratorConfig,
            CloseableIterator<Row> sourceIterator) throws IteratorCreationException {
        this.inputIterator = requireNonNull(sourceIterator);
        this.outputIterator = applyIterator(
                objectFactory,
                sleeperSchema,
                sleeperIteratorClassName,
                sleeperIteratorConfig,
                this.inputIterator);
    }

    /**
     * Apply the Sleeper iterator.
     *
     * @param  objectFactory             the {@link ObjectFactory} to use to create the Sleeper iterator
     * @param  sleeperSchema             the Sleeper {@link Schema} of the {@link Row} objects
     * @param  sleeperIteratorClassName  the Sleeper iterator to apply
     * @param  sleeperIteratorConfig     the configuration for the Sleeper iterator
     * @param  sourceIterator            the {@link CloseableIterator} to provide the source {@link Row} objects
     * @return                           the record iterator, with the Sleeper iterator applied
     * @throws IteratorCreationException if there was a failure creating the Sleeper iterator
     */
    private static CloseableIterator<Row> applyIterator(
            ObjectFactory objectFactory,
            Schema sleeperSchema,
            String sleeperIteratorClassName,
            String sleeperIteratorConfig,
            CloseableIterator<Row> sourceIterator) throws IteratorCreationException {
        if (null != sleeperIteratorClassName) {
            SortedRowIterator iterator;
            try {
                iterator = objectFactory.getObject(sleeperIteratorClassName, SortedRowIterator.class);
            } catch (ObjectFactoryException e) {
                throw new IteratorCreationException("ObjectFactoryException creating iterator of class " + sleeperIteratorClassName, e);
            }
            LOGGER.debug("Created iterator of class {}", sleeperIteratorClassName);
            iterator.init(sleeperIteratorConfig, sleeperSchema);
            LOGGER.debug("Initialised iterator with config {}", sleeperIteratorConfig);
            return iterator.apply(sourceIterator);
        }
        return sourceIterator;
    }

    @Override
    public boolean hasNext() {
        return outputIterator.hasNext();
    }

    @Override
    public Row next() {
        return outputIterator.next();
    }

    @Override
    public void close() throws IOException {
        inputIterator.close();
        outputIterator.close();
    }
}
