package sleeper.ingest.impl.recordbatch.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;

import java.util.NoSuchElementException;

/**
 * This class is a {@link CloseableIterator} of {@link Record}s, where these records are generated from an Apache Arrow
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
     * @param temporaryBufferAllocator The {@link BufferAllocator} to use as a working buffer
     * @param vectorSchemaRoot         The Arrow data to sort and iterate through
     * @param sleeperSchema            The Sleeper {@link Schema} corresponding to the columns of the {@link
     *                                 VectorSchemaRoot}
     */
    public RecordIteratorOrderedFromVectorSchemaRoot(BufferAllocator temporaryBufferAllocator,
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
        // Read the value from each field at row sortOrder(currentRecordNo) and use these values to construct a Record object.
        int noOfFields = vectorSchemaRoot.getSchema().getFields().size();
        int rowNoToRead = sortOrder.get(currentRecordNo);
        Record record = new Record();
        for (int fieldNo = 0; fieldNo < noOfFields; fieldNo++) {
            FieldVector fieldVector = vectorSchemaRoot.getVector(fieldNo);
            Object value = fieldVector.getObject(rowNoToRead);
            if (value instanceof Text) {
                value = value.toString();
            }
            record.put(fieldVector.getName(), value);
        }
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

