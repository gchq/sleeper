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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.MergingIterator;
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
import sleeper.core.util.LoggedDuration;
import sleeper.ingest.runner.impl.recordbatch.RecordBatch;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Stores a batch of records in Arrow format. These are held in-memory where possible, then spilled to disk once the
 * memory is full.
 * <p>
 * The ingest process works as follows:
 * <ul>
 * <li>Data is provided to this class through the {@link #append} methods. These are stored in a
 * {@link VectorSchemaRoot}</li>
 * <li>When an {@link org.apache.arrow.memory.OutOfMemoryException} occurs, the Arrow data is sorted and written to a
 * local file in Arrow format, and the {@link VectorSchemaRoot} is cleared to receive new data</li>
 * <li>The batch is deemed to be full when the total amount of data on the local disk exceeds a threshold</li>
 * <li>To retrieve the data, a {@link MergingIterator} is used to create one iterator of records from those local Arrow
 * files. No more data may be appended at this stage</li>
 * <li>The record batch cannot be reused and {@link #close} will delete all of the local files and free the memory</li>
 * </ul>
 * <p>
 * Subclasses of this class are responsible for implementing the {@link #append} method to take objects of type
 * {@link INCOMINGDATATYPE} and append the data to the internal {@link VectorSchemaRoot}. Note that the entire
 * contents of each {@link INCOMINGDATATYPE} are added to the {@link VectorSchemaRoot} in one go. Therefore if
 * the contents of the {@link INCOMINGDATATYPE} do not fit within the {@link VectorSchemaRoot} then the data
 * in the {@link VectorSchemaRoot} will be flushed and then retried. If the contents will never fit in the
 * {@link VectorSchemaRoot} then no progress can be made. It will be necessary to either increase the amount
 * of memory in the {@link VectorSchemaRoot} or to decrease the size of the {@link INCOMINGDATATYPE}.
 * <p>
 * Note that this class does not currently support Sleeper {@link MapType} or {@link ListType} fields.
 *
 * @param <INCOMINGDATATYPE> The type of data that can be appended to this record batch
 */
public class ArrowRecordBatch<INCOMINGDATATYPE> implements RecordBatch<INCOMINGDATATYPE> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArrowRecordBatch.class);
    private static final DecimalFormat FORMATTER = new DecimalFormat("0.#");
    public static final String MAP_KEY_FIELD_NAME = "key";
    public static final String MAP_VALUE_FIELD_NAME = "value";
    private static final int INITIAL_ARROW_VECTOR_CAPACITY = 1024;

    protected final VectorSchemaRoot vectorSchemaRoot;
    private final ArrowRecordWriter<INCOMINGDATATYPE> recordMapper;
    protected final BufferAllocator workingBufferAllocator;
    protected final BufferAllocator batchBufferAllocator;
    protected final Schema sleeperSchema;
    protected final List<Field> allFields;
    protected final String localWorkingDirectory;
    protected final int maxNoOfRecordsToWriteToArrowFileAtOnce;
    protected final List<String> localArrowFileNames;
    protected final String uniqueIdentifier;
    protected final long maxNoOfBytesToWriteLocally;
    protected int currentInsertIndex;
    protected long noOfBytesInLocalFiles;
    protected int currentBatchNo;
    protected CloseableIterator<Record> internalSortedRecordIterator;
    protected boolean isWriteable;

    /**
     * Construct an instance. Should be called by an {@link ArrowRecordBatchFactory}.
     *
     * @param arrowBufferAllocator                   the {@link BufferAllocator} to use to allocate memory for this
     *                                               buffer
     * @param sleeperSchema                          The Sleeper {@link Schema} of the records to be stored
     * @param localWorkingDirectory                  The local directory to use to store the spilled Arrow files
     * @param workingArrowBufferAllocatorBytes       The size of the working buffer, which is used for sorting and small
     *                                               batches during the writing process
     * @param minBatchArrowBufferAllocatorBytes      The minimum size of the buffer to hold the main batch of data. If
     *                                               this amount of data is unavailable then the object cannot be
     *                                               constructed
     * @param maxBatchArrowBufferAllocatorBytes      The maximum size of the buffer to hold the main batch of data. This
     *                                               may be shared with other processes and so the data may be flushed
     *                                               to local disk before this object has entirely filled it
     * @param maxNoOfBytesToWriteLocally             The maximum number of bytes to write to a local disk before this
     *                                               batch is considered full (approximate only)
     * @param maxNoOfRecordsToWriteToArrowFileAtOnce The Arrow file writing process writes multiple small batches of
     *                                               data of this size into a single file, to reduced the memory
     *                                               footprint
     */
    @SuppressFBWarnings("MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR")
    public ArrowRecordBatch(BufferAllocator arrowBufferAllocator,
            Schema sleeperSchema,
            ArrowRecordWriter<INCOMINGDATATYPE> recordMapper,
            String localWorkingDirectory,
            long workingArrowBufferAllocatorBytes,
            long minBatchArrowBufferAllocatorBytes,
            long maxBatchArrowBufferAllocatorBytes,
            long maxNoOfBytesToWriteLocally,
            int maxNoOfRecordsToWriteToArrowFileAtOnce) {
        requireNonNull(arrowBufferAllocator);
        this.sleeperSchema = requireNonNull(sleeperSchema);
        this.recordMapper = requireNonNull(recordMapper);
        this.allFields = sleeperSchema.getAllFields(); // This is an efficiency as getAllFields() is quite expensive
        this.localWorkingDirectory = requireNonNull(localWorkingDirectory);
        this.maxNoOfBytesToWriteLocally = maxNoOfBytesToWriteLocally;
        this.maxNoOfRecordsToWriteToArrowFileAtOnce = maxNoOfRecordsToWriteToArrowFileAtOnce;
        this.currentBatchNo = 0;
        this.currentInsertIndex = 0;
        this.noOfBytesInLocalFiles = 0L;
        this.localArrowFileNames = new ArrayList<>();
        this.uniqueIdentifier = UUID.randomUUID().toString();
        this.internalSortedRecordIterator = null;
        this.isWriteable = true;

        try {
            // Create two Arrow buffer allocators, as children of the supplied parent allocator
            //  - The working buffer has a fixed size and it is preallocated here.
            //  - The batch buffer has a minimum size, which is preallocated here. It also has a maximum size, but the
            //    parent allocator may not have enough memory available to provide this and an Arrow OutOfMemoryException
            //    will be thrown when it tries to expand beyond the available capacity. (Note that this is
            //    different from the standard Java OutOfMemoryError).
            this.workingBufferAllocator = arrowBufferAllocator.newChildAllocator(
                    "Working buffer",
                    workingArrowBufferAllocatorBytes,
                    workingArrowBufferAllocatorBytes);
            this.batchBufferAllocator = arrowBufferAllocator.newChildAllocator(
                    "Batch buffer",
                    minBatchArrowBufferAllocatorBytes,
                    maxBatchArrowBufferAllocatorBytes);
            // Create an Arrow VectorSchemaRoot object to hold the in-memory batch of records.
            // Allocating memory for these vectors is slightly fiddly as there is no
            // VectorSchemaRoot.allocateNew(capacity) method. Note that an initial allocation does not prevent
            // additional allocations later on, as records with a variable width may take up more space than expected.
            // Follow the Arrow pattern of create > allocate > mutate > set value count > access > clear
            // Here we do the create > allocate
            org.apache.arrow.vector.types.pojo.Schema arrowSchema = convertSleeperSchemaToArrowSchema(sleeperSchema);
            this.vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, this.batchBufferAllocator);
            this.vectorSchemaRoot.getFieldVectors().forEach(fieldVector -> fieldVector.setInitialCapacity(INITIAL_ARROW_VECTOR_CAPACITY));
            this.vectorSchemaRoot.allocateNew();
        } catch (Exception e1) {
            try {
                this.close();
            } catch (Exception e2) {
                e1.addSuppressed(e2);
            }
            throw e1;
        }
        LOGGER.info("Created ArrowRecordBatchBase with:\n"
                + "\tschema of {}\n\tlocalWorkingDirectory of {}\n\tworkingArrowBufferAllocatorBytes of {}\n"
                + "\tminBatchArrowBufferAllocatorBytes of {}\n\tmaxBatchArrowBufferAllocatorBytes of {}\n"
                + "\tmaxNoOfBytesToWriteLocally of {}\n\tmaxNoOfRecordsToWriteToArrowFileAtOnce of {}",
                this.sleeperSchema, this.localWorkingDirectory, workingArrowBufferAllocatorBytes,
                minBatchArrowBufferAllocatorBytes, maxBatchArrowBufferAllocatorBytes,
                this.maxNoOfBytesToWriteLocally, this.maxNoOfRecordsToWriteToArrowFileAtOnce);
    }

    /**
     * Sort a set of Arrow vectors according to a Sleeper schema and write to a local Arrow file. The rows are written
     * out in small batches to minimise the amount of additional memory that is required.
     *
     * @param  temporaryBufferAllocator               the buffer allocator to use for working memory
     * @param  sleeperSchema                          the Sleeper {@link Schema} of the rows to be sorted
     * @param  sourceVectorSchemaRoot                 the {@link VectorSchemaRoot} containing the rows to be written
     * @param  localArrowFileName                     the name of the file to write the Arrow data to
     * @param  maxNoOfRecordsToWriteToArrowFileAtOnce the Arrow file writing process writes multiple small batches of
     *                                                data of this size into a single file, to reduce the memory
     *                                                footprint
     * @return                                        number of bytes written
     * @throws IOException                            if there was a failure writing the local Arrow file
     */
    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private static long sortArrowAndWriteToLocalFile(BufferAllocator temporaryBufferAllocator,
            Schema sleeperSchema,
            VectorSchemaRoot sourceVectorSchemaRoot,
            String localArrowFileName,
            int maxNoOfRecordsToWriteToArrowFileAtOnce) throws IOException {
        int sourceVectorSize = sourceVectorSchemaRoot.getRowCount();
        List<Field> allSleeperFields = sleeperSchema.getAllFields();
        // Determine the order in which the rows are to be written to the Arrow file
        // Create a VectorSchemaRoot to hold each small batch before it is written
        // Open an output channel to write to the destination file
        // Create a writer to write the small batches into the output stream
        long bytesWritten;
        Path arrowFilePath = Paths.get(localArrowFileName);
        Path arrowFileParent = Objects.requireNonNull(arrowFilePath.getParent());
        Files.createDirectories(arrowFileParent);
        LOGGER.debug("Determining sort order and opening local arrow file");
        try (IntVector wholeFileSortOrderVector = ArrowIngestSupport.createSortOrderVector(temporaryBufferAllocator, sleeperSchema, sourceVectorSchemaRoot);
                VectorSchemaRoot smallBatchVectorSchemaRoot = VectorSchemaRoot.create(sourceVectorSchemaRoot.getSchema(), temporaryBufferAllocator);
                FileChannel outputFileChannel = FileChannel.open(arrowFilePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(smallBatchVectorSchemaRoot, null, outputFileChannel)) {
            // Allocate memory in the vectors underlying the VectorSchemaRoot and start the writer
            smallBatchVectorSchemaRoot.getFieldVectors().forEach(fieldVector -> fieldVector.setInitialCapacity(maxNoOfRecordsToWriteToArrowFileAtOnce));
            smallBatchVectorSchemaRoot.allocateNew();
            arrowStreamWriter.start();
            // Write a slice of maxNoOfRecordsToWriteToArrowFileAtOnce rows at a time
            int sliceStart = 0;
            int sliceNo = 0;
            while (sliceStart < sourceVectorSize) {
                if (sliceNo % 1000 == 0) {
                    LOGGER.debug(String.format("Writing slice number %05d (starting at row number %09d of %09d)", sliceNo, sliceStart, sourceVectorSize));
                }
                // Calculate the bounds for the current slice
                int sliceEnd = sliceStart + maxNoOfRecordsToWriteToArrowFileAtOnce;
                if (sliceEnd > sourceVectorSize) {
                    sliceEnd = sourceVectorSize;
                }
                int sliceLength = sliceEnd - sliceStart;
                // Populate the smallBatchVectorSchemaRoot from the sourceVectorSchemaRoot,
                // taking the sort order into account
                for (int sliceIndex = 0; sliceIndex < sliceLength; sliceIndex++) {
                    int readIndex = wholeFileSortOrderVector.get(sliceStart + sliceIndex);
                    for (int fieldNo = 0; fieldNo < allSleeperFields.size(); fieldNo++) {
                        ValueVector srcVector = sourceVectorSchemaRoot.getVector(fieldNo);
                        ValueVector dstVector = smallBatchVectorSchemaRoot.getVector(fieldNo);
                        dstVector.copyFromSafe(readIndex, sliceIndex, srcVector);
                    }
                    smallBatchVectorSchemaRoot.setRowCount(sliceIndex + 1);
                }
                // Write the batch
                arrowStreamWriter.writeBatch();
                // Prepare for the next batch
                sliceStart = sliceEnd;
                sliceNo++;
            }
            arrowStreamWriter.end();
            bytesWritten = arrowStreamWriter.bytesWritten();
            // The sort vector, smallBatchVectorSchemaRoot, channel and writer are auto-closed at the end of the try block
        }
        LOGGER.debug(String.format("Written %09d bytes", bytesWritten));
        return bytesWritten;
    }

    /**
     * Open an iterator of records reading from a named Arrow file. The iterator should be closed by the caller when it
     * is no longer needed.
     *
     * @param  bufferAllocator    the Arrow {@link BufferAllocator} to use as a working buffer during file-reading
     * @param  localArrowFileName the Arrow file to read
     * @return                    an iterator of records read from the Arrow file
     * @throws IOException        if there was a failure reading from or writing to the Arrow file
     */
    private static CloseableIterator<Record> createCloseableRecordIteratorForArrowFile(BufferAllocator bufferAllocator,
            String localArrowFileName) throws IOException {
        FileChannel inputFileChannel = FileChannel.open(Paths.get(localArrowFileName), StandardOpenOption.READ);
        ArrowStreamReader arrowStreamReader = new ArrowStreamReader(inputFileChannel, bufferAllocator);
        return new RecordIteratorFromArrowStreamReader(arrowStreamReader);
    }

    /**
     * Create an Arrow Schema from a Sleeper Schema. The order of the fields in each Schema is retained.
     *
     * @param  sleeperSchema The Sleeper {@link Schema}
     * @return               The Arrow {@link org.apache.arrow.vector.types.pojo.Schema}
     */
    private static org.apache.arrow.vector.types.pojo.Schema convertSleeperSchemaToArrowSchema(Schema sleeperSchema) {
        List<org.apache.arrow.vector.types.pojo.Field> arrowFields = sleeperSchema.getAllFields().stream()
                .map(ArrowRecordBatch::convertSleeperFieldToArrowField)
                .collect(Collectors.toList());
        return new org.apache.arrow.vector.types.pojo.Schema(arrowFields);
    }

    private static org.apache.arrow.vector.types.pojo.Field convertSleeperFieldToArrowField(Field sleeperField) {
        String fieldName = sleeperField.getName();
        Type sleeperType = sleeperField.getType();
        if (sleeperType instanceof IntType ||
                sleeperType instanceof LongType ||
                sleeperType instanceof StringType ||
                sleeperType instanceof ByteArrayType) {
            // Where the Sleeper field type is a straightforward primitive type, the corresponding Arrow type is used
            return convertSleeperPrimitiveFieldToArrowField(sleeperField);
        } else if (sleeperType instanceof ListType) {
            // Where the Sleeper field type is a list, the Arrow field type is also a list. The elements of the
            // Arrow list are chosen to match the (primitive) type of the elements in the Sleeper list
            Type elementSleeperType = ((ListType) sleeperType).getElementType();
            Field elementSleeperField = new Field("element", elementSleeperType);
            org.apache.arrow.vector.types.pojo.Field elementArrowField = convertSleeperPrimitiveFieldToArrowField(elementSleeperField);
            return new org.apache.arrow.vector.types.pojo.Field(
                    fieldName,
                    new org.apache.arrow.vector.types.pojo.FieldType(false, new ArrowType.List(), null),
                    Collections.singletonList(elementArrowField));
        } else if (sleeperType instanceof MapType) {
            // Where the Sleeper field type is a map, the Arrow field type is a list. Each element of the list is an
            // Arrow struct with two members: key and value. The types of the key and value are chosen to match the
            // (primitive) type of the elements in the Sleeper list.
            // This implementation does not use the Arrow 'map' field type, as we were unable to make this approach work
            // in our experiments.
            Type keySleeperType = ((MapType) sleeperType).getKeyType();
            Type valueSleeperType = ((MapType) sleeperType).getValueType();
            Field keySleeperField = new Field(MAP_KEY_FIELD_NAME, keySleeperType);
            Field valueSleeperField = new Field(MAP_VALUE_FIELD_NAME, valueSleeperType);
            org.apache.arrow.vector.types.pojo.Field keyArrowField = convertSleeperPrimitiveFieldToArrowField(keySleeperField);
            org.apache.arrow.vector.types.pojo.Field valueArrowField = convertSleeperPrimitiveFieldToArrowField(valueSleeperField);
            org.apache.arrow.vector.types.pojo.Field elementArrowStructField = new org.apache.arrow.vector.types.pojo.Field(
                    fieldName + "-key-value-struct",
                    new org.apache.arrow.vector.types.pojo.FieldType(false, new ArrowType.Struct(), null),
                    Stream.of(keyArrowField, valueArrowField).collect(Collectors.toList()));
            return new org.apache.arrow.vector.types.pojo.Field(
                    fieldName,
                    new org.apache.arrow.vector.types.pojo.FieldType(false, new ArrowType.List(), null),
                    Collections.singletonList(elementArrowStructField));
        } else {
            throw new UnsupportedOperationException("Sleeper column type " + sleeperType.toString() + " is not handled");
        }
    }

    /**
     * Convert a primitive Sleeper field into an Arrow field.
     *
     * @param  sleeperField The Sleeper field to be converted
     * @return              The corresponding Arrow field
     */
    private static org.apache.arrow.vector.types.pojo.Field convertSleeperPrimitiveFieldToArrowField(Field sleeperField) {
        String fieldName = sleeperField.getName();
        Type sleeperType = sleeperField.getType();
        if (sleeperType instanceof IntType) {
            return org.apache.arrow.vector.types.pojo.Field.notNullable(fieldName, new ArrowType.Int(32, true));
        } else if (sleeperType instanceof LongType) {
            return org.apache.arrow.vector.types.pojo.Field.notNullable(fieldName, new ArrowType.Int(64, true));
        } else if (sleeperType instanceof StringType) {
            return org.apache.arrow.vector.types.pojo.Field.notNullable(fieldName, new ArrowType.Utf8());
        } else if (sleeperType instanceof ByteArrayType) {
            return org.apache.arrow.vector.types.pojo.Field.notNullable(fieldName, new ArrowType.Binary());
        } else {
            throw new AssertionError("Sleeper column type " + sleeperType.toString() + " is not a primitive inside convertSleeperPrimitiveFieldToArrowField()");
        }
    }

    /**
     * Close this object, closing the internal record iterator (if present), freeing memory and deleting local Arrow
     * files.
     */
    @Override
    public void close() {
        isWriteable = false;
        if (internalSortedRecordIterator != null) {
            try {
                internalSortedRecordIterator.close();
            } catch (Exception e) {
                LOGGER.error("Error closing internalSortedRecordIterator", e);
            }
        }
        internalSortedRecordIterator = null;
        vectorSchemaRoot.close();
        workingBufferAllocator.close();
        batchBufferAllocator.close();
        deleteAllLocalArrowFiles();
    }

    /**
     * Delete all of the local Arrow files.
     */
    private void deleteAllLocalArrowFiles() {
        if (!localArrowFileNames.isEmpty()) {
            LOGGER.debug("Deleting {} local batch files, first: {} last: {}",
                    localArrowFileNames.size(),
                    localArrowFileNames.get(0),
                    localArrowFileNames.get(localArrowFileNames.size() - 1));
            localArrowFileNames.forEach(localFileName -> {
                try {
                    if (!(new File(localFileName)).delete()) {
                        LOGGER.error("Failed to delete local file {}", localFileName);
                    }
                } catch (Exception e) {
                    LOGGER.error("Failed to delete local file {}: {}", localFileName, e.toString());
                }
            });
        }
    }

    /**
     * Flush the in-memory Arrow data to a new local file, clearing the Arrow batch and updating the internal counters.
     *
     * @throws IOException if there was a failure writing the local file
     */
    protected void flushToLocalArrowFileThenClear() throws IOException {
        if (currentInsertIndex <= 0) {
            throw new AssertionError("A request was made to flush to disk when there were no records in memory. "
                    + "The most likely reason for this is that the contents of the incoming data type are too big "
                    + "to fit within the in memory buffer. Either increase the size of the memory buffer or reduce "
                    + "the number of records in the incoming data type.");
        }
        String localFileName = constructLocalFileNameForBatch(currentBatchNo);
        // Follow the Arrow pattern of create > allocate > mutate > set value count > access > clear
        // Here we do the set value count > access > clear
        Instant startTime = Instant.now();
        LOGGER.debug("Writing {} records to local Arrow file {}", currentInsertIndex, localFileName);
        long bytesWrittenToLocalFile;
        try {
            bytesWrittenToLocalFile = sortArrowAndWriteToLocalFile(
                    workingBufferAllocator,
                    sleeperSchema,
                    vectorSchemaRoot,
                    localFileName,
                    maxNoOfRecordsToWriteToArrowFileAtOnce);
        } catch (Exception e) {
            LOGGER.warn("An exception occurred during sortArrowAndWriteToLocalFile", e);
            throw e;
        }
        LoggedDuration duration = LoggedDuration.withShortOutput(startTime, Instant.now());
        LOGGER.info("Wrote {} records ({} bytes) to local Arrow file in {} ({}/s) - filename: {}",
                currentInsertIndex,
                bytesWrittenToLocalFile,
                duration,
                FORMATTER.format(currentInsertIndex / (double) duration.getSeconds()),
                localFileName);
        vectorSchemaRoot.clear();
        currentInsertIndex = 0;
        // Record the local file name for later, and update the counters
        localArrowFileNames.add(localFileName);
        noOfBytesInLocalFiles += bytesWrittenToLocalFile;
        LOGGER.info("Total number of bytes written to local files is {}", noOfBytesInLocalFiles);
        currentBatchNo++;
    }

    private String constructLocalFileNameForBatch(int batchNo) {
        return String.format("%s/localfile-%s-batch-%s.arrow",
                localWorkingDirectory,
                uniqueIdentifier,
                batchNo);
    }

    /**
     * Create an iterator to iterate through all of the records in this batch. This method may only be called once,
     * after which the returned iterator is the only way to read the data and no more data may be written.
     *
     * @return An iterator to iterate through all of the records in sorted order.
     */
    @Override
    public CloseableIterator<Record> createOrderedRecordIterator() throws IOException {
        if (!isWriteable || (internalSortedRecordIterator != null)) {
            throw new AssertionError("Attempt to create an iterator where an iterator has already been created");
        }
        isWriteable = false;
        try {
            internalSortedRecordIterator = createSortedRecordIterator();
            return internalSortedRecordIterator;
        } catch (Exception e1) {
            try {
                close();
            } catch (Exception e2) {
                e1.addSuppressed(e2);
            }
            throw e1;
        }
    }

    /**
     * Create an iterator to iterate through all of the records in this batch, in sort order. It flushes the current
     * in-memory batch to disk and clears the memory, then performs a merge-sort of every local Arrow file (each of
     * which were sorted as it was written).
     *
     * @return             An iterator to iterate through all of the records in sorted order.
     * @throws IOException if there was a failure writing the local file
     */
    private CloseableIterator<Record> createSortedRecordIterator() throws IOException {
        if (currentInsertIndex > 0) {
            LOGGER.debug("Creating an iterator: flushing memory to disk");
            flushToLocalArrowFileThenClear();
        }
        // Log this action
        LOGGER.info("Starting merge-sort of {} local files", localArrowFileNames.size());
        if (!localArrowFileNames.isEmpty()) {
            LOGGER.debug("First file: {} last file: {}",
                    localArrowFileNames.get(0),
                    localArrowFileNames.get(localArrowFileNames.size() - 1));
        }
        // Create the variables for the iterators here so that they can be closed if an error occurs
        List<CloseableIterator<Record>> sortedRecordIteratorsToMerge = new ArrayList<>(localArrowFileNames.size());
        try {
            // Create an iterator from each local file
            localArrowFileNames.forEach(localFileName -> {
                try {
                    sortedRecordIteratorsToMerge.add(createCloseableRecordIteratorForArrowFile(workingBufferAllocator, localFileName));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            // Merge the local file iterators into one sorted iterator
            return new MergingIterator(sleeperSchema, sortedRecordIteratorsToMerge);
        } catch (Exception e1) {
            // Clean up carefully if something went wrong during the creation of this iterator
            sortedRecordIteratorsToMerge.forEach(iterator -> {
                try {
                    iterator.close();
                } catch (Exception e2) {
                    e1.addSuppressed(e2);
                }
            });
            throw e1;
        }
    }

    @Override
    public void append(INCOMINGDATATYPE data) throws IOException {
        if (!isWriteable) {
            throw new AssertionError();
        }

        // Add the record to the major batch of records (stored as an Arrow VectorSchemaRoot)
        // If the addition to the major batch causes an Arrow out-of-memory error then flush the batch to local
        // disk and then try adding the record again.
        boolean writeRequired = true;
        while (writeRequired) {
            try {
                currentInsertIndex = recordMapper.insert(allFields, vectorSchemaRoot, data, currentInsertIndex);
                writeRequired = false;
            } catch (OutOfMemoryException e) {
                LOGGER.debug("OutOfMemoryException occurred whilst writing a Record: flushing and retrying");
                flushToLocalArrowFileThenClear();
            }
        }
    }

    @Override
    public boolean isFull() {
        return noOfBytesInLocalFiles > maxNoOfBytesToWriteLocally;
    }

}
