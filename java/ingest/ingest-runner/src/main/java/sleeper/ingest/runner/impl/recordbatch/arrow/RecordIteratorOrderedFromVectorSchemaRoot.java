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
package sleeper.ingest.runner.impl.recordbatch.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;

import java.util.NoSuchElementException;

/**
 * An iterator of Sleeper records generated from Apache Arrow vectors. These records are generated from an Apache Arrow
 * {@link VectorSchemaRoot}. The rows are sorted before they are returned, according to the row keys and sort keys
 * specified in the supplied Sleeper {@link Schema}.
 */
class RecordIteratorOrderedFromVectorSchemaRoot implements CloseableIterator<Record> {
    private final VectorSchemaRoot vectorSchemaRoot;
    private final IntVector sortOrder;
    private int currentRecordNo = 0;

    /**
     * Construct a SortedRecordIteratorFromVectorSchemaRoot.
     *
     * @param temporaryBufferAllocator the {@link BufferAllocator} to use as a working buffer
     * @param vectorSchemaRoot         the Arrow data to sort and iterate through
     * @param sleeperSchema            the Sleeper {@link Schema} corresponding to the columns of the
     *                                 {@link VectorSchemaRoot}
     */
    RecordIteratorOrderedFromVectorSchemaRoot(
            BufferAllocator temporaryBufferAllocator,
            VectorSchemaRoot vectorSchemaRoot,
            Schema sleeperSchema) {
        this.vectorSchemaRoot = vectorSchemaRoot;
        this.sortOrder = ArrowIngestSupport.createSortOrderVector(temporaryBufferAllocator, sleeperSchema, this.vectorSchemaRoot);
    }

    @Override
    public boolean hasNext() {
        return (currentRecordNo < vectorSchemaRoot.getRowCount());
    }

    @Override
    public Record next() throws NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        // Read the value from row sortOrder(currentRecordNo) and use that row to construct a Record object.
        int rowNoToRead = sortOrder.get(currentRecordNo);
        Record record = ArrowToRecordConversionUtils.convertVectorSchemaRootToRecord(vectorSchemaRoot, rowNoToRead);
        currentRecordNo++;
        return record;
    }

    /**
     * This closes and frees all internal data.
     */
    @Override
    public void close() {
        sortOrder.close();
    }

    public long getNumberOfRecordsRead() {
        return currentRecordNo;
    }
}
