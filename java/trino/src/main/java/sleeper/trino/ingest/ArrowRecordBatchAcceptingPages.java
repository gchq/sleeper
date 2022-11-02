/*
 * Copyright 2022 Crown Copyright
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchBase;

import java.io.IOException;

/**
 * This class extends {@link ArrowRecordBatchBase} so that it accepts data as {@link Page} objects.
 */
public class ArrowRecordBatchAcceptingPages extends ArrowRecordBatchBase<Page> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArrowRecordBatchAcceptingPages.class);

    /**
     * Construct an {@link ArrowRecordBatchAcceptingPages} object.
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
    public ArrowRecordBatchAcceptingPages(BufferAllocator arrowBufferAllocator,
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
     * Add a single Page to a VectorSchemaRoot, starting at a specified row.
     * <p>
     * Note that the field order in the supplied Sleeper Schema must match the field order in the Arrow Schema within
     * the {@link VectorSchemaRoot} argument.
     *
     * @param sleeperSchema      The Sleeper {@link Schema} of the page that is being written
     * @param vectorSchemaRoot   The Arrow store to write into
     * @param page               The {@link Page} of data to write
     * @param startInsertAtRowNo The index of the first row to write
     * @throws OutOfMemoryException When the {@link BufferAllocator} associated with the {@link VectorSchemaRoot} cannot
     *                              provide enough memory
     */
    private static void addPageToVectorSchemaRoot(Schema sleeperSchema,
                                                  VectorSchemaRoot vectorSchemaRoot,
                                                  Page page,
                                                  int startInsertAtRowNo) throws OutOfMemoryException {
        // Follow the Arrow pattern of create > allocate > mutate > set value count > access > clear
        // Here we do the mutate
        // Note that setSafe() is used throughout so that more memory will be requested if required.
        // An OutOfMemoryException is thrown if this fails.
        int noOfPositions = page.getPositionCount();
        int noOfFields = sleeperSchema.getAllFields().size();
        for (int fieldNo = 0; fieldNo < noOfFields; fieldNo++) {
            Field sleeperField = sleeperSchema.getAllFields().get(fieldNo);
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
        vectorSchemaRoot.setRowCount(startInsertAtRowNo + page.getPositionCount());
    }

    @Override
    public void append(Page page) throws IOException {
        if (!isWriteable) {
            throw new AssertionError();
        }
        // Add the record to the major batch of records (stored as an Arrow VectorSchemaRoot)
        // If the addition to the major batch causes an Arrow out-of-memory error then flush the batch to local
        // disk and then try adding the record again.
        boolean writeRequired = true;
        while (writeRequired) {
            try {
                addPageToVectorSchemaRoot(
                        super.sleeperSchema,
                        super.vectorSchemaRoot,
                        page,
                        super.currentInsertIndex);
                super.currentInsertIndex += page.getPositionCount();
                writeRequired = false;
            } catch (OutOfMemoryException e) {
                super.flushToLocalArrowFileThenClear();
            }
        }
    }
}
