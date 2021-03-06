package sleeper.ingest.impl.recordbatch.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
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
import java.util.Map;

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
     *                                               data of this size into a single file, to reduce the memory
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
            } else if (sleeperType instanceof ListType) {
                writeList(
                        ((ListType) sleeperType).getElementType(),
                        (List<?>) record.get(fieldName),
                        (ListVector) vectorSchemaRoot.getVector(fieldNo),
                        insertAtRowNo);
            } else if (sleeperType instanceof MapType) {
                writeMap(
                        ((MapType) sleeperType).getKeyType(),
                        ((MapType) sleeperType).getValueType(),
                        (Map<?, ?>) record.get(fieldName),
                        (ListVector) vectorSchemaRoot.getVector(fieldNo),
                        insertAtRowNo);
            } else {
                throw new UnsupportedOperationException("Sleeper column type " + sleeperType.toString() + " is not handled");
            }
        }
        vectorSchemaRoot.setRowCount(insertAtRowNo + 1);
    }

    private static void writeList(Type sleeperElementType,
                                  List<?> listOfValues,
                                  ListVector listVector,
                                  int insertAtRowNo) {
        BufferAllocator bufferAllocator = listVector.getAllocator();
        UnionListWriter unionListWriter = listVector.getWriter();
        unionListWriter.setPosition(insertAtRowNo);
        unionListWriter.startList();
        listOfValues.forEach(value -> writeListElement(bufferAllocator, unionListWriter, sleeperElementType, value));
        unionListWriter.endList();
        if (listOfValues.isEmpty()) {
            // This call is counterintuitive, but surprisingly it prevents a problem where any vector which contains at
            // least one empty array always appears to contain empty strings. This solution was discovered by accident,
            // which is extremely unsatisfactory. It works in Arrow 8.0.0. It is possible that it will have unexpected
            // consequences.
            // The documentation for Arrow 8.0.0 is sparse and it may be that this call is masking a programming error
            // elsewhere in this code. Alternatively, it may be an error in the Arrow code base.
            unionListWriter.setValueCount(1);
        } else {
            unionListWriter.setValueCount(listOfValues.size());
        }
    }

    private static void writeMap(Type sleeperKeyType,
                                 Type sleeperValueType,
                                 Map<?, ?> mapOfValues,
                                 ListVector listOfMapEntryStructs,
                                 int insertAtRowNo) {
        // Maps are written to Arrow as a list of structs, where each struct has two fields: key and value.
        // The Arrow Map type is not used because we could not get it to work in our experiments.
        BufferAllocator bufferAllocator = listOfMapEntryStructs.getAllocator();
        UnionListWriter unionListWriter = listOfMapEntryStructs.getWriter();
        unionListWriter.setPosition(insertAtRowNo);
        unionListWriter.startList();
        mapOfValues.forEach((key, value) -> {
            BaseWriter.StructWriter structWriter = unionListWriter.struct();
            structWriter.start();
            writeStructElement(bufferAllocator, unionListWriter, sleeperKeyType, key, MAP_KEY_FIELD_NAME);
            writeStructElement(bufferAllocator, unionListWriter, sleeperValueType, value, MAP_VALUE_FIELD_NAME);
            structWriter.end();
        });
        unionListWriter.endList();
        if (mapOfValues.isEmpty()) {
            // This call is counterintuitive, but surprisingly it prevents a problem where any vector which contains at
            // least one empty array always appears to contain empty strings. This solution was discovered by accident,
            // which is extremely unsatisfactory. It works in Arrow 8.0.0. It is possible that it will have unexpected
            // consequences.
            // The documentation for Arrow 8.0.0 is sparse and it may be that this call is masking a programming error
            // elsewhere in this code. Alternatively, it may be an error in the Arrow code base.
            unionListWriter.setValueCount(1);
        } else {
            unionListWriter.setValueCount(mapOfValues.size());
        }
    }

    private static void writeListElement(BufferAllocator bufferAllocator,
                                         UnionListWriter unionListWriter,
                                         Type sleeperElementType,
                                         Object objectToWrite) {
        if (sleeperElementType instanceof IntType) {
            unionListWriter.writeInt((int) objectToWrite);
        } else if (sleeperElementType instanceof LongType) {
            unionListWriter.writeBigInt((long) objectToWrite);
        } else if (sleeperElementType instanceof StringType) {
            byte[] bytes = ((String) objectToWrite).getBytes(StandardCharsets.UTF_8);
            try (ArrowBuf arrowBuf = bufferAllocator.buffer(bytes.length)) {
                arrowBuf.setBytes(0, bytes);
                unionListWriter.writeVarChar(0, bytes.length, arrowBuf);
            }
        } else if (sleeperElementType instanceof ByteArrayType) {
            byte[] bytes = (byte[]) objectToWrite;
            try (ArrowBuf arrowBuf = bufferAllocator.buffer(bytes.length)) {
                arrowBuf.setBytes(0, bytes);
                unionListWriter.writeVarBinary(0, bytes.length, arrowBuf);
            }
        } else {
            throw new AssertionError("Sleeper column type " + sleeperElementType.toString() + " is an element type sent to writeListElement()");
        }
    }

    private static void writeStructElement(BufferAllocator bufferAllocator,
                                           BaseWriter.StructWriter structWriter,
                                           Type sleeperElementType,
                                           Object objectToWrite,
                                           String structFieldName) {
        if (sleeperElementType instanceof IntType) {
            structWriter.integer(structFieldName).writeInt((int) objectToWrite);
        } else if (sleeperElementType instanceof LongType) {
            structWriter.bigInt(structFieldName).writeBigInt((long) objectToWrite);
        } else if (sleeperElementType instanceof StringType) {
            byte[] bytes = ((String) objectToWrite).getBytes(StandardCharsets.UTF_8);
            try (ArrowBuf arrowBuf = bufferAllocator.buffer(bytes.length)) {
                arrowBuf.setBytes(0, bytes);
                structWriter.varChar(structFieldName).writeVarChar(0, bytes.length, arrowBuf);
            }
        } else if (sleeperElementType instanceof ByteArrayType) {
            byte[] bytes = (byte[]) objectToWrite;
            try (ArrowBuf arrowBuf = bufferAllocator.buffer(bytes.length)) {
                arrowBuf.setBytes(0, bytes);
                structWriter.varBinary(structFieldName).writeVarBinary(0, bytes.length, arrowBuf);
            }
        } else {
            throw new AssertionError("Sleeper column type " + sleeperElementType.toString() + " is an element type sent to writeStructElement()");
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
