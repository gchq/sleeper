package sleeper.ingest.impl.recordbatch.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.Text;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.record.Record;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * This class is a {@link CloseableIterator} of {@link Record}s, where those records are read from a {@link
 * ArrowStreamReader}.
 * <p>
 * The rows are read from the file in small batches, which correspond to the small batches that were used when the file
 * was orginally written.
 */
class RecordIteratorFromArrowStreamReader implements CloseableIterator<Record> {
    private final ArrowStreamReader arrowStreamReader;
    private int currentRecordNoInBatch;
    private long totalNoOfRecordsRead = 0L;
    private boolean nextBatchLoaded;

    /**
     * Construct an ArrowStreamIterator.
     *
     * @param arrowStreamReader The {@link ArrowStreamReader} to use to read the small batchea from the file.
     * @throws IOException -
     */
    public RecordIteratorFromArrowStreamReader(ArrowStreamReader arrowStreamReader) throws IOException {
        this.arrowStreamReader = arrowStreamReader;
        this.loadNextBatch();
    }

    /**
     * Instruct the {@link ArrowStreamReader} to read the next small batch of rows from the file, into its internal
     * {@link VectorSchemaRoot}
     *
     * @throws IOException -
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
            // Retrieve the current small batch from within the ArrowStreamReader, read the value from each field
            // at row currentRecordNoInBatch and use these values to construct a Record object.
            VectorSchemaRoot smallBatchVectorSchemaRoot = arrowStreamReader.getVectorSchemaRoot();
            int noOfFields = smallBatchVectorSchemaRoot.getSchema().getFields().size();
            Record record = new Record();
            for (int fieldNo = 0; fieldNo < noOfFields; fieldNo++) {
                FieldVector fieldVector = smallBatchVectorSchemaRoot.getVector(fieldNo);
                Object value = fieldVector.getObject(currentRecordNoInBatch);
                if (value instanceof Text) {
                    value = value.toString();
                }
                record.put(fieldVector.getName(), value);
            }
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

