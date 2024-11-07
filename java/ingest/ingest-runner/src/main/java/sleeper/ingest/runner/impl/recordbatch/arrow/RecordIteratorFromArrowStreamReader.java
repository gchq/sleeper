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

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.record.Record;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Reads through records read into memory with Arrow. This is a {@link CloseableIterator} of {@link Record}s, where
 * those records are read from a {@link ArrowStreamReader}.
 * <p>
 * The rows are read from the file in small batches, which correspond to the small batches that were used when the file
 * was orginally written.
 */
class RecordIteratorFromArrowStreamReader implements CloseableIterator<Record> {
    private final ArrowStreamReader arrowStreamReader;
    private int currentRecordNoInBatch;
    private long totalNoOfRecordsRead = 0L;
    private boolean nextBatchLoaded;

    RecordIteratorFromArrowStreamReader(ArrowStreamReader arrowStreamReader) throws IOException {
        this.arrowStreamReader = arrowStreamReader;
        this.loadNextBatch();
    }

    /**
     * Read the next small batch of rows from the source file. Instruct the {@link ArrowStreamReader} to read into its
     * internal {@link VectorSchemaRoot}.
     *
     * @throws IOException if there was a failure reading a batch from the source file
     */
    private void loadNextBatch() throws IOException {
        nextBatchLoaded = arrowStreamReader.loadNextBatch();
        currentRecordNoInBatch = 0;
    }

    @Override
    public boolean hasNext() {
        try {
            // The most recent batch must have been loaded and it must have at least one row
            return nextBatchLoaded && arrowStreamReader.getVectorSchemaRoot().getRowCount() > 0;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Record next() throws NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        try {
            // Retrieve the current small batch from within the ArrowStreamReader, read the value from
            // row currentRecordNoInBatch and use these values to construct a Record object.
            VectorSchemaRoot smallBatchVectorSchemaRoot = arrowStreamReader.getVectorSchemaRoot();
            Record record = ArrowToRecordConversionUtils.convertVectorSchemaRootToRecord(smallBatchVectorSchemaRoot, currentRecordNoInBatch);
            currentRecordNoInBatch++;
            totalNoOfRecordsRead++;
            // Load a new batch when this one has been read fully
            if (currentRecordNoInBatch >= smallBatchVectorSchemaRoot.getRowCount()) {
                loadNextBatch();
            }
            return record;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        arrowStreamReader.close();
    }

    public long getNumberOfRecordsRead() {
        return totalNoOfRecordsRead;
    }
}
