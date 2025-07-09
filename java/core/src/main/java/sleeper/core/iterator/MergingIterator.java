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

import sleeper.core.record.Row;
import sleeper.core.record.SleeperRowComparator;
import sleeper.core.schema.Schema;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Merges a list of sorted iterators into one fully sorted iterator. This is done by using a {@link PriorityQueue} where
 * the smallest record is returned first.
 * <p>
 * Note: for performance reasons this does not check that the given iterators are sorted. As this class is only used
 * internally it should never be called with non-sorted iterators.
 */
public class MergingIterator implements CloseableIterator<Row> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MergingIterator.class);

    private final List<CloseableIterator<Row>> inputIterators;
    private final PriorityQueue<RecordIteratorPair> queue;
    private long recordsRead;

    public MergingIterator(Schema schema, List<CloseableIterator<Row>> inputIterators) {
        this.inputIterators = inputIterators;
        this.recordsRead = 0L;
        this.queue = new PriorityQueue<>(new RecordIteratorPairComparator(schema));
        for (CloseableIterator<Row> iterator : inputIterators) {
            if (iterator.hasNext()) {
                queue.add(new RecordIteratorPair(iterator.next(), iterator));
                this.recordsRead++;
            }
        }
    }

    @Override
    public boolean hasNext() {
        return !queue.isEmpty();
    }

    @Override
    public Row next() {
        RecordIteratorPair pair = queue.poll();
        if (pair.iterator.hasNext()) {
            RecordIteratorPair newPair = new RecordIteratorPair(pair.iterator.next(), pair.iterator);
            queue.add(newPair);
            recordsRead++;
            if (0 == recordsRead % 1_000_000) {
                LOGGER.info("Read {} records", recordsRead);
            }
        }
        return pair.record;
    }

    @Override
    public void close() throws IOException {
        for (CloseableIterator<Row> iterator : inputIterators) {
            iterator.close();
        }
    }

    public long getNumberOfRecordsRead() {
        return recordsRead;
    }

    /**
     * Holds the next record available for an iterator, and the iterator to retrieve further records.
     */
    private static class RecordIteratorPair {
        private final Row record;
        private final CloseableIterator<Row> iterator;

        RecordIteratorPair(Row record, CloseableIterator<Row> iterator) {
            this.record = record;
            this.iterator = iterator;
        }
    }

    /**
     * Compares the state for two iterators to find the next record in the sort order.
     */
    private static class RecordIteratorPairComparator implements Comparator<RecordIteratorPair> {
        private final SleeperRowComparator sleeperRowComparator;

        RecordIteratorPairComparator(Schema schema) {
            this.sleeperRowComparator = new SleeperRowComparator(schema);
        }

        @Override
        public int compare(RecordIteratorPair pair1, RecordIteratorPair pair2) {
            return sleeperRowComparator.compare(pair1.record, pair2.record);
        }
    }
}
