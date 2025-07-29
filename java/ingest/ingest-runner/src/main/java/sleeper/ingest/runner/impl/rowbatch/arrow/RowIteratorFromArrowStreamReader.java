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
package sleeper.ingest.runner.impl.rowbatch.arrow;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.row.Row;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Reads through rows read into memory with Arrow. This is a {@link CloseableIterator} of {@link Row}s, where
 * those rows are read from a {@link ArrowStreamReader}.
 * <p>
 * The rows are read from the file in small batches, which correspond to the small batches that were used when the file
 * was orginally written.
 */
class RowIteratorFromArrowStreamReader implements CloseableIterator<Row> {
    private final ArrowStreamReader arrowStreamReader;
    private int currentRowNumInBatch;
    private boolean nextBatchLoaded;

    RowIteratorFromArrowStreamReader(ArrowStreamReader arrowStreamReader) throws IOException {
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
        currentRowNumInBatch = 0;
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
    public Row next() throws NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        try {
            // Retrieve the current small batch from within the ArrowStreamReader, read the values from the
            // vector and use these values to construct a row object.
            VectorSchemaRoot smallBatchVectorSchemaRoot = arrowStreamReader.getVectorSchemaRoot();
            Row row = ArrowToRowConversionUtils.convertVectorSchemaRootToRow(smallBatchVectorSchemaRoot, currentRowNumInBatch);
            currentRowNumInBatch++;
            // Load a new batch when this one has been read fully
            if (currentRowNumInBatch >= smallBatchVectorSchemaRoot.getRowCount()) {
                loadNextBatch();
            }
            return row;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        arrowStreamReader.close();
    }
}
