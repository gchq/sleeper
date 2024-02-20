/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.ingest.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.iterator.SortedRecordIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * This class is an iterator of Records, taken from a stream of {@link Record} objects obtained from another iterator
 * and then having a Sleeper iterator applied, such as a compaction iterator.
 * <p>
 * If the Sleeper iterator requires the records to be in a specific order then the source iterator must supply them in
 * that order.
 */
class RecordIteratorWithSleeperIteratorApplied implements CloseableIterator<Record> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordIteratorWithSleeperIteratorApplied.class);

    private final CloseableIterator<Record> inputIterator;
    private final CloseableIterator<Record> outputIterator;

    /**
     * Construct a RecordIteratorWithSleeperIteratorApplied.
     *
     * @param objectFactory            The {@link ObjectFactory} to use to create the Sleeper iterator
     * @param sleeperSchema            The Sleeper {@link Schema} of the {@link Record} objects
     * @param sleeperIteratorClassName The Sleeper iterator to apply
     * @param sleeperIteratorConfig    The configuration for the Sleeper iterator
     * @param sourceIterator           The {@link CloseableIterator} to provide the source {@link Record} objects
     * @throws IteratorException Thrown when there is an error in the Sleeper iterator
     */
    RecordIteratorWithSleeperIteratorApplied(
            ObjectFactory objectFactory,
            Schema sleeperSchema,
            String sleeperIteratorClassName,
            String sleeperIteratorConfig,
            CloseableIterator<Record> sourceIterator) throws IteratorException {
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
     * @param objectFactory            The {@link ObjectFactory} to use to create the Sleeper iterator
     * @param sleeperSchema            The Sleeper {@link Schema} of the {@link Record} objects
     * @param sleeperIteratorClassName The Sleeper iterator to apply
     * @param sleeperIteratorConfig    The configuration for the Sleeper iterator
     * @param sourceIterator           The {@link CloseableIterator} to provide the source {@link Record} objects
     * @return The record iterator, with the Sleeper iterator applied
     * @throws IteratorException Thrown when there is an error in the Sleeper iterator
     */
    private static CloseableIterator<Record> applyIterator(
            ObjectFactory objectFactory,
            Schema sleeperSchema,
            String sleeperIteratorClassName,
            String sleeperIteratorConfig,
            CloseableIterator<Record> sourceIterator) throws IteratorException {
        if (null != sleeperIteratorClassName) {
            SortedRecordIterator iterator;
            try {
                iterator = objectFactory.getObject(sleeperIteratorClassName, SortedRecordIterator.class);
            } catch (ObjectFactoryException e) {
                throw new IteratorException("ObjectFactoryException creating iterator of class " + sleeperIteratorClassName, e);
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
    public Record next() {
        return outputIterator.next();
    }

    @Override
    public void close() throws IOException {
        inputIterator.close();
        outputIterator.close();
    }
}
