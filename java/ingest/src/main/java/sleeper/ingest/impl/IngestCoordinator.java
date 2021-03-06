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
package sleeper.ingest.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriter;
import sleeper.ingest.impl.recordbatch.RecordBatch;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static sleeper.core.metrics.MetricsLogger.METRICS_LOGGER;

/**
 * This class writes data to Sleeper partition files.
 * <p>
 * The ingest process works as follows:
 * <ul>
 *     <li>Data is provided to this class through the {@link #write(Object)} method. These data may be supplied as any data type are stored in a {@link RecordBatch} for that data type</li>
 *     <li>When the {@link RecordBatch} is full, the data is retrieved from the {@link RecordBatch} as {@link Record} objects, in sorted order</li>
 *     <li>The sorted rows are passed to an {@link IngesterIntoPartitions} object, which uses {@link PartitionFileWriter} objects to create the partition files in the appropriate file system, possibly asynchronously</li>
 *     <li>Once all of the partition files have been created, the Sleeper {@link StateStore} is updated to include the new partition files</li>
 *     <li>The {@link RecordBatch}</li> is cleared, its resources freed, and a new one is created to accept more data</li>
 *     <li>So long as this {@link IngestCoordinator} remains open, more data can be supplied and more partition files will be created if required</li>
 *     <li>When this {@link IngestCoordinator} is closed, any remaining data is written to partition files and a {@link CompletableFuture} is returned that will complete once all of the files have been fully ingested and any 
intermediate files removed</li>
 * </ul>
 * <p>
 * The {@link RecordBatch} and the {@link PartitionFileWriter} to use are specified using factory functions that create
 * objects which satisfy the relevant interfaces.
 *
 * @param <INCOMINGDATATYPE> The type of data that the class accepts.
 */
public class IngestCoordinator<INCOMINGDATATYPE> implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestCoordinator.class);
    private static final long PARTITIONS_NEVER_UPDATED_TIME = -1;

    private final ObjectFactory objectFactory;
    private final StateStore sleeperStateStore;
    private final Schema sleeperSchema;
    private final String sleeperIteratorClassName;
    private final String sleeperIteratorConfig;
    private final int ingestPartitionRefreshFrequencyInSeconds;
    private final Supplier<RecordBatch<INCOMINGDATATYPE>> createNewRecordBatchFn;
    private final IngesterIntoPartitions ingesterIntoPartitions;

    private final List<CompletableFuture<List<FileInfo>>> ingestFutures;
    private final long ingestCoordinatorCreationTime;
    protected RecordBatch<INCOMINGDATATYPE> currentRecordBatch;
    private long lastPartitionsUpdateTime;
    private PartitionTree partitionTree;
    private boolean isClosed;

    /**
     * Construct an {@link IngestCoordinator} object.
     *
     * @param objectFactory                            The Sleeper {@link ObjectFactory} to use to create Sleeper
     *                                                 iterators
     * @param sleeperStateStore                        The Sleeper state store
     * @param sleeperSchema                            The Sleeper schema of the data
     * @param sleeperIteratorClassName                 The Sleeper iterator class name
     * @param sleeperIteratorConfig                    The Sleeper iterator configuration
     * @param ingestPartitionRefreshFrequencyInSeconds The number of seconds to wait before the current list of
     *                                                 partitions is refreshed from the state store
     * @param recordBatchFactoryFn                     A function to use to create new {@link RecordBatch} objects
     * @param partitionFileWriterFactoryFn             A function to use to create new {@link PartitionFileWriter}
     *                                                 objects
     */
    public IngestCoordinator(ObjectFactory objectFactory,
                             StateStore sleeperStateStore,
                             Schema sleeperSchema,
                             String sleeperIteratorClassName,
                             String sleeperIteratorConfig,
                             int ingestPartitionRefreshFrequencyInSeconds,
                             Supplier<RecordBatch<INCOMINGDATATYPE>> recordBatchFactoryFn,
                             Function<Partition, PartitionFileWriter> partitionFileWriterFactoryFn) {
        LOGGER.info("Creating IngestCoordinator with schema of {}", sleeperSchema);

        // Supplied member variables
        this.objectFactory = requireNonNull(objectFactory);
        this.sleeperStateStore = requireNonNull(sleeperStateStore);
        this.sleeperSchema = requireNonNull(sleeperSchema);
        this.sleeperIteratorClassName = sleeperIteratorClassName;
        this.sleeperIteratorConfig = sleeperIteratorConfig;
        this.ingestPartitionRefreshFrequencyInSeconds = ingestPartitionRefreshFrequencyInSeconds;
        this.createNewRecordBatchFn = requireNonNull(recordBatchFactoryFn);

        // Other member variables
        this.ingestCoordinatorCreationTime = System.currentTimeMillis();
        this.lastPartitionsUpdateTime = PARTITIONS_NEVER_UPDATED_TIME;
        this.ingestFutures = new ArrayList<>();
        this.ingesterIntoPartitions = new IngesterIntoPartitions(sleeperSchema, partitionFileWriterFactoryFn);
        this.currentRecordBatch = this.createNewRecordBatchFn.get();
        this.isClosed = false;
    }

    /**
     * Updates the Sleeper {@link StateStore} with the details of new data files which are available. This method will
     * repeatedly retry if the update fails, with an exponential backoff.
     *
     * @param sleeperStateStore The state store to update
     * @param fileInfoList      The details of the files to add to the state store
     * @throws StateStoreException  -
     * @throws InterruptedException -
     */
    private static void updateStateStore(StateStore sleeperStateStore,
                                         List<FileInfo> fileInfoList)
            throws StateStoreException, InterruptedException {
        boolean success = false;
        int numberOfFailures = 0;
        while (!success) {
            try {
                sleeperStateStore.addFiles(fileInfoList);
                success = true;
            } catch (StateStoreException e) {
                LOGGER.error("Failed to update DynamoDB with new files", e);
                numberOfFailures++;
                if (numberOfFailures >= 10) {
                    throw new StateStoreException("Unable to update StateStore after 10 attempts (most recent exception was " + e.getMessage() + ")", e);
                }
                // Sleep with exponential back-off
                Thread.sleep((long) Math.pow(2.0D, numberOfFailures));
            }
        }
    }

    /**
     * Check to see whether the current {@link RecordBatch} is full, and if it is, retrieve the records from the batch
     * in sorted order, apply a Sleeper iterator if required, split the sorted data into partitions and ingest the
     * partitions into the back-end store.
     *
     * @param isClosing Indicates that the {@link IngestCoordinator} is closing, so force the ingest, even if the record
     *                  batch is not full, and do not recreate internal data structures
     * @throws IOException         -
     * @throws IteratorException   -
     * @throws StateStoreException -
     */
    private void initiateIngestIfNecessary(boolean isClosing)
            throws StateStoreException, IteratorException, IOException {
        // If the record batch is full, or it is closing, then initiate the ingest
        if (isClosing || currentRecordBatch.isFull()) {
            // Update view of partitions if necessary
            updatePartitionTreeIfNecessary();
            // Apply the Sleeper iterator to the record batch, within a try-with-resources block. This will ensure that
            // the iterators are closed in both success and failure
            try (CloseableIterator<Record> orderedRecordIteratorFromBatch = currentRecordBatch.createOrderedRecordIterator();
                 CloseableIterator<Record> recordIteratorWithSleeperIteratorApplied =
                         new RecordIteratorWithSleeperIteratorApplied(
                                 objectFactory,
                                 sleeperSchema,
                                 sleeperIteratorClassName,
                                 sleeperIteratorConfig,
                                 orderedRecordIteratorFromBatch)) {
                // Create a future which completes once the partitions are created, the records ingested
                // and the state store updated.
                // Note that once initiateIngest() has been called, below, the record batch has been consumed and is no
                // longer required.
                CompletableFuture<List<FileInfo>> consumedFuture = ingesterIntoPartitions
                        .initiateIngest(recordIteratorWithSleeperIteratorApplied, partitionTree)
                        .thenApply(fileInfoList -> {
                            // Update the state store
                            try {
                                updateStateStore(sleeperStateStore, fileInfoList);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            // Return the total number of records that ave been written
                            return fileInfoList;
                        });
                ingestFutures.add(consumedFuture);
            }
            // The record batch has now been consumed and so close it.
            currentRecordBatch.close();
            currentRecordBatch = (isClosing) ? null : createNewRecordBatchFn.get();
        }
    }

    /**
     * Queries the Sleeper {@link StateStore} to retrieve the current partition tree, if too much time has elapsed since
     * the last refresh.
     *
     * @throws StateStoreException -
     */
    private void updatePartitionTreeIfNecessary() throws StateStoreException {
        int secondsSinceUpdated = (int) ((System.currentTimeMillis() - lastPartitionsUpdateTime) / 1000.0);
        if (lastPartitionsUpdateTime == PARTITIONS_NEVER_UPDATED_TIME ||
                secondsSinceUpdated > ingestPartitionRefreshFrequencyInSeconds) {
            if (lastPartitionsUpdateTime == PARTITIONS_NEVER_UPDATED_TIME) {
                LOGGER.info("Updating list of leaf partitions for the first time");
            } else {
                LOGGER.info("Updating list of leaf partitions as {} seconds since last updated", secondsSinceUpdated);
            }
            List<Partition> allPartitions = sleeperStateStore.getAllPartitions();
            partitionTree = new PartitionTree(sleeperSchema, allPartitions);
            lastPartitionsUpdateTime = System.currentTimeMillis();
            LOGGER.info("There are {} partitions", allPartitions.size());
        }
    }

    /**
     * Close this ingester synchronously, as required by {@link AutoCloseable}. This is the only closing method that may
     * be called more than once. Second and subsequent calls are ignored and a warning is logged.
     * <p>
     * This method uses {@link #asyncCloseReturningFileInfoList()} and waits for that future to complete before
     * returning.
     *
     * @throws IOException         -
     * @throws IteratorException   -
     * @throws StateStoreException -
     */
    @Override
    public void close() throws StateStoreException, IteratorException, IOException {
        if (isClosed) {
            LOGGER.warn("Closing an IngestCoordinator that has already been closed");
        } else {
            asyncCloseReturningFileInfoList().join();
        }
    }

    /**
     * Close this ingester synchronously, returning information about every file that was ingested and added to the
     * state store. This method uses {@link #asyncCloseReturningFileInfoList()} and waits for that future to complete
     * before returning.
     *
     * @return Details about every file that was added to the state store.
     * @throws IOException         -
     * @throws IteratorException   -
     * @throws StateStoreException -
     */
    public List<FileInfo> closeReturningFileInfoList() throws StateStoreException, IteratorException, IOException {
        return asyncCloseReturningFileInfoList().join();
    }

    /**
     * Close this ingester asynchronously, returning information about every partition file that was ingested and added
     * to the state store. Closing the ingester causes any data that has been ingested to be sorted, partitioned and
     * uploaded to the back-end storage, and the state store to be updated. The returned {@link CompletableFuture} will
     * only resolve once this is complete and all intermediate resources have been freed.
     *
     * @return A {@link CompletableFuture} which resolves to a list of information about every file that was added to
     * the state store.
     * @throws IOException         -
     * @throws IteratorException   -
     * @throws StateStoreException -
     */
    public CompletableFuture<List<FileInfo>> asyncCloseReturningFileInfoList()
            throws StateStoreException, IteratorException, IOException {
        if (isClosed) {
            throw new AssertionError("Attempt to close IngestCoordinator and return results twice");
        }
        isClosed = true;
        // Ingest any data remaining in the current RecordBatch
        initiateIngestIfNecessary(true);
        // There are many futures which have been created. Create a future which waits for them all to complete
        // and then returns a flattened list of all of the FileInfo objects which were passed to the state store
        return CompletableFuture.allOf(ingestFutures.toArray(new CompletableFuture[0]))
                .whenComplete((msg, ex) -> internalClose())
                .thenApply(dummy -> {
                    List<FileInfo> fileInfoList = ingestFutures.stream()
                            .map(CompletableFuture::join)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList());
                    long noOfRecordsWritten = fileInfoList.stream()
                            .mapToLong(FileInfo::getNumberOfRecords)
                            .sum();
                    double elapsedSeconds = (System.currentTimeMillis() - ingestCoordinatorCreationTime) / 1000.0;
                    METRICS_LOGGER.info(String.format("Wrote %d records to S3 in %.1f seconds at %.1f per second",
                            noOfRecordsWritten,
                            elapsedSeconds,
                            noOfRecordsWritten / elapsedSeconds));
                    return fileInfoList;
                });
    }

    /**
     * Abort the ingest process, including any uploads which are in progress.
     */
    public void abort() {
        LOGGER.info("Aborting ingest");
        ingestFutures.forEach(future -> future.cancel(true));
        internalClose();
        LOGGER.info("Ingest aborted");
    }

    /**
     * Release internal data structures.
     */
    private void internalClose() {
        if (currentRecordBatch != null) {
            try {
                currentRecordBatch.close();
            } catch (Exception e) {
                LOGGER.error("Failed to close currentRecordBatch", e);
            }
        }
        currentRecordBatch = null;
    }

    /**
     * Write data to this ingester.
     * <p>
     * When this method is called, it may initiate significant activity such as sorting the data that is held in memory
     * and flushing it to local disk, or merging local files and saving them as partition files on a remote file store.
     * The amount of time taken by a call to this function varies significantly.
     *
     * @param data The data to ingest
     * @throws StateStoreException -
     * @throws IteratorException   -
     * @throws IOException         -
     */
    public void write(INCOMINGDATATYPE data) throws StateStoreException, IteratorException, IOException {
        try {
            initiateIngestIfNecessary(false);
            currentRecordBatch.append(data);
        } catch (Exception e) {
            internalClose();
            throw e;
        }
    }
}
