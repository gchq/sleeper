package sleeper.ingest.impl.recordbatch.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.*;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * This class extends {@link ArrowRecordBatchBase} so that it accepts data as {@link Record} objects.
 */
public class ArrowRecordBatchAcceptingRecords extends ArrowRecordBatchBase<Record> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArrowRecordBatchAcceptingRecords.class);

    /**
     * Construct a {@link ArrowRecordBatchAcceptingRecords} object.
     *
     * @param arrowBufferAllocator                   The {@link BufferAllocator} to use to allocate memory
     * @param sleeperSchema                          The Sleeper {@link Schema} of the records to be stored
     * @param localWorkingDirectory                  The local directory to use to store the spilled Arrow files
     * @param workingArrowBufferAllocatorBytes       -
     * @param minBatchArrowBufferAllocatorBytes      -
     * @param maxBatchArrowBufferAllocatorBytes      -
     * @param maxNoOfBytesToWriteLocally             -
     * @param maxNoOfRecordsToWriteToArrowFileAtOnce The Arrow file writing process writes multiple small batches of
     *                                               data of this size into a single file, to reduced the memory
     *                                               footprint
     */
    public ArrowRecordBatchAcceptingRecords(BufferAllocator arrowBufferAllocator,
                                            Schema sleeperSchema,
                                            String localWorkingDirectory,
                                            long workingArrowBufferAllocatorBytes,
                                            long minBatchArrowBufferAllocatorBytes,
                                            long maxBatchArrowBufferAllocatorBytes,
                                            long maxNoOfBytesToWriteLocally,
                                            int maxNoOfRecordsToWriteToArrowFileAtOnce) {
        super(arrowBufferAllocator,
                sleeperSchema,
                localWorkingDirectory,
                workingArrowBufferAllocatorBytes,
                minBatchArrowBufferAllocatorBytes,
                maxBatchArrowBufferAllocatorBytes,
                maxNoOfBytesToWriteLocally,
                maxNoOfRecordsToWriteToArrowFileAtOnce);
    }

    /**
     * Add a single Record to a VectorSchemaRoot at a specified row.
     * <p>
     * Note that the field order in the supplied Sleeper Schema must match the field order in the Arrow Schema within
     * the {@link VectorSchemaRoot} argument.
     *
     * @param allFields        The result of {@link Schema#getAllFields()} of the record that is being written. This is
     *                         used instead of the raw {@link Schema} as the {@link Schema#getAllFields()} is a bit too
     *                         expensive to call on every record
     * @param vectorSchemaRoot The Arrow store to write into
     * @param record           The {@link Record} to write
     * @param insertAtRowNo    The row number to write to
     * @throws OutOfMemoryException When the {@link BufferAllocator} associated with the {@link VectorSchemaRoot} cannot
     *                              provide enough memory
     */
    private static void addRecordToVectorSchemaRoot(List<Field> allFields,
                                                    VectorSchemaRoot vectorSchemaRoot,
                                                    Record record,
                                                    int insertAtRowNo) throws OutOfMemoryException {
        // Follow the Arrow pattern of create > allocate > mutate > set value count > access > clear
        // Here we do the mutate
        // Note that setSafe() is used throughout so that more memory will be requested if required.
        // An OutOfMemoryException is thrown if this fails.
        for (int fieldNo = 0; fieldNo < allFields.size(); fieldNo++) {
            Field sleeperField = allFields.get(fieldNo);
            String fieldName = sleeperField.getName();
            Type sleeperType = sleeperField.getType();
            if (sleeperType instanceof IntType) {
                IntVector intVector = (IntVector) vectorSchemaRoot.getVector(fieldNo);
                Integer value = (Integer) record.get(fieldName);
                intVector.setSafe(insertAtRowNo, value);
            } else if (sleeperType instanceof LongType) {
                BigIntVector bigIntVector = (BigIntVector) vectorSchemaRoot.getVector(fieldNo);
                Long value = (Long) record.get(fieldName);
                bigIntVector.setSafe(insertAtRowNo, value);
            } else if (sleeperType instanceof StringType) {
                VarCharVector varCharVector = (VarCharVector) vectorSchemaRoot.getVector(fieldNo);
                String value = (String) record.get(fieldName);
                varCharVector.setSafe(insertAtRowNo, value.getBytes(StandardCharsets.UTF_8));
            } else if (sleeperType instanceof ByteArrayType) {
                VarBinaryVector varBinaryVector = (VarBinaryVector) vectorSchemaRoot.getVector(fieldNo);
                byte[] value = (byte[]) record.get(fieldName);
                varBinaryVector.setSafe(insertAtRowNo, value);
            } else {
                throw new UnsupportedOperationException("Sleeper column type " + sleeperType.toString() + " is not handled");
            }
        }
    }

    @Override
    public void append(Record record) throws IOException {
        if (!isWriteable) {
            throw new AssertionError();
        }
        // Add the record to the major batch of records (stored as an Arrow VectorSchemaRoot)
        // If the addition to the major batch causes an Arrow out-of-memory error then flush the batch to local
        // disk and then try adding the record again.
        boolean writeRequired = true;
        while (writeRequired) {
            try {
                addRecordToVectorSchemaRoot(
                        super.allFields,
                        super.vectorSchemaRoot,
                        record,
                        super.currentInsertIndex);
                super.currentInsertIndex++;
                writeRequired = false;
            } catch (OutOfMemoryException e) {
                LOGGER.debug("OutOfMemoryException occurred whilst writing a Record: flushing and retrying");
                super.flushToLocalArrowFileThenClear();
            }
        }
    }

    public void write(Iterator<Record> recordIterator)
            throws IOException, IteratorException, InterruptedException, StateStoreException, OutOfMemoryException {
        // Add each record from the iterator in turn
        while (recordIterator.hasNext()) {
            append(recordIterator.next());
        }
    }
}
