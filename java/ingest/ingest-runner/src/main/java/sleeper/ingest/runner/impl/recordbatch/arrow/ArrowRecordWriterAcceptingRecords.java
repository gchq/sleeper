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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;

import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static sleeper.ingest.runner.impl.recordbatch.arrow.ArrowRecordBatch.MAP_KEY_FIELD_NAME;
import static sleeper.ingest.runner.impl.recordbatch.arrow.ArrowRecordBatch.MAP_VALUE_FIELD_NAME;

/**
 * Accepts data for an Arrow record batch as Sleeper records. Used by {@link ArrowRecordBatch}.
 */
public class ArrowRecordWriterAcceptingRecords implements ArrowRecordWriter<Record> {

    /**
     * Add a single Record to a VectorSchemaRoot at a specified row. Note that the field order in the supplied Sleeper
     * Schema must match the field order in the Arrow Schema within the {@link VectorSchemaRoot} argument.
     *
     * @param  allFields            The result of {@link Schema#getAllFields()} of the record that is being written.
     *                              This is used instead of the raw {@link Schema} as the {@link Schema#getAllFields()}
     *                              is a bit too expensive to call on every record.
     * @param  vectorSchemaRoot     the Arrow store to write into
     * @param  record               the {@link Record} to write
     * @param  insertAtRowNo        the row number to write to
     * @return                      the row number to use when this method is next called
     * @throws OutOfMemoryException when the {@link BufferAllocator} associated with the {@link VectorSchemaRoot} cannot
     *                              provide enough memory
     */
    @Override
    public int insert(List<Field> allFields,
            VectorSchemaRoot vectorSchemaRoot,
            Record record,
            int insertAtRowNo) throws OutOfMemoryException {
        writeRecord(allFields, vectorSchemaRoot, record, insertAtRowNo);
        int finalRowCount = insertAtRowNo + 1;
        vectorSchemaRoot.setRowCount(finalRowCount);
        return finalRowCount;
    }

    public static void writeRecord(
            List<Field> allFields, VectorSchemaRoot vectorSchemaRoot, Record record, int insertAtRowNo) {
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
    }

    private static void writeList(
            Type sleeperElementType, List<?> listOfValues, ListVector listVector, int insertAtRowNo) {
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

    private static void writeMap(
            Type sleeperKeyType, Type sleeperValueType,
            Map<?, ?> mapOfValues, ListVector listOfMapEntryStructs, int insertAtRowNo) {
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

    private static void writeListElement(
            BufferAllocator bufferAllocator, UnionListWriter unionListWriter,
            Type sleeperElementType, Object objectToWrite) {
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

    private static void writeStructElement(
            BufferAllocator bufferAllocator, BaseWriter.StructWriter structWriter, Type sleeperElementType,
            Object objectToWrite, String structFieldName) {
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

}
