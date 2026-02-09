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
package sleeper.trino.ingest;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;
import sleeper.ingest.runner.impl.rowbatch.arrow.ArrowRowWriter;

import java.util.List;

/**
 * A row writer to accept data as Trino pages. Implements {@link ArrowRowWriter}.
 */
public class ArrowRowWriterAcceptingPages implements ArrowRowWriter<Page> {

    /**
     * Add a single Page to a VectorSchemaRoot, starting at a specified row. Note that the field order in the supplied
     * Sleeper schema must match the field order in the Arrow schema within the {@link VectorSchemaRoot} argument.
     *
     * @param  allFields            The result of {@link Schema#getAllFields()} of the record that is being written.
     *                              This is used instead of the raw {@link Schema} as the {@link Schema#getAllFields()}
     *                              is a bit too expensive to call on every record.
     * @param  vectorSchemaRoot     the Arrow store to write into
     * @param  page                 the {@link Page} of data to write
     * @param  startInsertAtRowNo   the index of the first row to write
     * @return                      the row number to use when this method is next called
     * @throws OutOfMemoryException when the {@link BufferAllocator} associated with the {@link VectorSchemaRoot} cannot
     *                              provide enough memory
     */
    public int insert(
            List<Field> allFields, VectorSchemaRoot vectorSchemaRoot,
            Page page, int startInsertAtRowNo) throws OutOfMemoryException {
        // Follow the Arrow pattern of create > allocate > mutate > set value count > access > clear
        // Here we do the mutate
        // Note that setSafe() is used throughout so that more memory will be requested if required.
        // An OutOfMemoryException is thrown if this fails.
        int noOfPositions = page.getPositionCount();
        int noOfFields = allFields.size();
        for (int fieldNo = 0; fieldNo < noOfFields; fieldNo++) {
            Field sleeperField = allFields.get(fieldNo);
            Type sleeperType = sleeperField.getType();
            Block block = page.getBlock(fieldNo);
            for (int positionNo = 0; positionNo < noOfPositions; positionNo++) {
                if (sleeperType instanceof IntType) {
                    IntVector intVector = (IntVector) vectorSchemaRoot.getVector(fieldNo);
                    int value = block.getInt(positionNo, 0);
                    intVector.setSafe(startInsertAtRowNo + positionNo, value);
                } else if (sleeperType instanceof LongType) {
                    BigIntVector bigIntVector = (BigIntVector) vectorSchemaRoot.getVector(fieldNo);
                    long value = block.getLong(positionNo, 0);
                    bigIntVector.setSafe(startInsertAtRowNo + positionNo, value);
                } else if (sleeperType instanceof StringType) {
                    VarCharVector varCharVector = (VarCharVector) vectorSchemaRoot.getVector(fieldNo);
                    byte[] value = block.getSlice(positionNo, 0, block.getSliceLength(positionNo)).getBytes();
                    varCharVector.setSafe(startInsertAtRowNo + positionNo, value);
                } else {
                    throw new UnsupportedOperationException("Sleeper column type " + sleeperType.toString() + " is not handled");
                }
            }
        }
        int finalRowCount = startInsertAtRowNo + noOfPositions;
        vectorSchemaRoot.setRowCount(finalRowCount);
        return finalRowCount;
    }

}
