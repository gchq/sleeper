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

import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.util.IteratorConfig;
import sleeper.core.util.IteratorFactory;
import sleeper.core.util.ObjectFactory;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Iterates over a source of rows with a Sleeper iterator applied to it. If the Sleeper iterator requires the rows
 * to be in a specific order then the source iterator must supply them in that order.
 */
class RowIteratorWithSleeperIteratorApplied implements CloseableIterator<Row> {
    private final CloseableIterator<Row> inputIterator;
    private final CloseableIterator<Row> outputIterator;

    /**
     * Create an instance.
     *
     * @param  objectFactory             the {@link ObjectFactory} to use to create the Sleeper iterator
     * @param  iteratorConfig            the {@link IteratorConfig} to use to create the Sleeper iterator
     * @param  schema                    the Sleeper {@link Schema} of the {@link Row} objects
     * @param  sourceIterator            the {@link CloseableIterator} to provide the source {@link Row} objects
     * @throws IteratorCreationException if there was a failure creating the Sleeper iterator
     */
    RowIteratorWithSleeperIteratorApplied(
            ObjectFactory objectFactory,
            IteratorConfig iteratorConfig,
            Schema schema,
            CloseableIterator<Row> sourceIterator) throws IteratorCreationException {
        this.inputIterator = requireNonNull(sourceIterator);
        this.outputIterator = applyIterator(
                objectFactory,
                iteratorConfig,
                schema,
                this.inputIterator);
    }

    /**
     * Apply the Sleeper iterator.
     *
     * @param  objectFactory             the {@link ObjectFactory} to use to create the Sleeper iterator
     * @param  iteratorConfig            the {@link IteratorConfig} to use to create the Sleeper iterator
     * @param  schema                    the Sleeper {@link Schema} of the {@link Row} objects
     * @param  sourceIterator            the {@link CloseableIterator} to provide the source {@link Row} objects
     * @return                           the row iterator, with the Sleeper iterator applied
     * @throws IteratorCreationException if there was a failure creating the Sleeper iterator
     */
    private static CloseableIterator<Row> applyIterator(
            ObjectFactory objectFactory,
            IteratorConfig iteratorConfig,
            Schema schema,
            CloseableIterator<Row> sourceIterator) throws IteratorCreationException {
        if (iteratorConfig.shouldIteratorBeApplied()) {
            return new IteratorFactory(objectFactory)
                    .getIterator(iteratorConfig, schema)
                    .apply(sourceIterator);
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
