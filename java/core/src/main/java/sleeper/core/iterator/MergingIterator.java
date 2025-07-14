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
import sleeper.core.row.RowComparator;
import sleeper.core.schema.Schema;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Merges a list of sorted iterators into one fully sorted iterator. This is done by using a {@link PriorityQueue} where
 * the smallest row is returned first.
 * <p>
 * Note: for performance reasons this does not check that the given iterators are sorted. As this class is only used
 * internally it should never be called with non-sorted iterators.
 */
public class MergingIterator implements CloseableIterator<Row> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MergingIterator.class);

    private final List<CloseableIterator<Row>> inputIterators;
    private final PriorityQueue<RowIteratorPair> queue;
    private long rowsRead;

    public MergingIterator(Schema schema, List<CloseableIterator<Row>> inputIterators) {
        this.inputIterators = inputIterators;
        this.rowsRead = 0L;
        this.queue = new PriorityQueue<>(new RowIteratorPairComparator(schema));
        for (CloseableIterator<Row> iterator : inputIterators) {
            if (iterator.hasNext()) {
                queue.add(new RowIteratorPair(iterator.next(), iterator));
                this.rowsRead++;
            }
        }
    }

    @Override
    public boolean hasNext() {
        return !queue.isEmpty();
    }

    @Override
    public Row next() {
        RowIteratorPair pair = queue.poll();
        if (pair.iterator.hasNext()) {
            RowIteratorPair newPair = new RowIteratorPair(pair.iterator.next(), pair.iterator);
            queue.add(newPair);
            rowsRead++;
            if (0 == rowsRead % 1_000_000) {
                LOGGER.info("Read {} rows", rowsRead);
            }
        }
        return pair.row;
    }

    @Override
    public void close() throws IOException {
        for (CloseableIterator<Row> iterator : inputIterators) {
            iterator.close();
        }
    }

    public long getNumberOfRowsRead() {
        return rowsRead;
    }

    /**
     * Holds the next row available for an iterator, and the iterator to retrieve further rows.
     */
    private static class RowIteratorPair {
        private final Row row;
        private final CloseableIterator<Row> iterator;

        RowIteratorPair(Row row, CloseableIterator<Row> iterator) {
            this.row = row;
            this.iterator = iterator;
        }
    }

    /**
     * Compares the state for two iterators to find the next row in the sort order.
     */
    private static class RowIteratorPairComparator implements Comparator<RowIteratorPair> {
        private final RowComparator rowComparator;

        RowIteratorPairComparator(Schema schema) {
            this.rowComparator = new RowComparator(schema);
        }

        @Override
        public int compare(RowIteratorPair pair1, RowIteratorPair pair2) {
            return rowComparator.compare(pair1.row, pair2.row);
        }
    }
}
