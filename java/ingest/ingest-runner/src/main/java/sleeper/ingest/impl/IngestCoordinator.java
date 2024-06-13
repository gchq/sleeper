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
package sleeper.ingest.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.validation.IngestFileWritingStrategy;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.LoggedDuration;
import sleeper.ingest.IngestResult;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.RecordBatch;
import sleeper.ingest.impl.recordbatch.RecordBatchFactory;

import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.core.metrics.MetricsLogger.METRICS_LOGGER;

/**
 * Writes data to Sleeper partition files. The ingest process works as follows:
 * <ul>
 * <li>
 * Data is provided to this class through the {@link #write(Object)} method. These data may be supplied as any data
 * type are stored in a {@link RecordBatch} for that data type.
 * </li>
 * <li>
 * When the {@link RecordBatch} is full, the data is retrieved from the {@link RecordBatch} as {@link Record} objects,
 * in sorted order.
 * </li>
 * <li>
 * The sorted rows are passed to an {@link IngesterIntoPartitions} object, which uses
 * {@link sleeper.ingest.impl.partitionfilewriter.PartitionFileWriter} objects to create the partition files in the
 * appropriate file system, possibly asynchronously.
 * </li>
 * <li>
 * Once all of the partition files have been created, the Sleeper {@link StateStore} is updated to include the new
 * partition files.
 * </li>
 * <li>
 * The {@link RecordBatch} is cleared, its resources freed, and a new one is created to accept more data.
 * </li>
 * <li>
 * So long as this {@link IngestCoordinator} remains open, more data can be supplied and more partition files will
 * be created if required.
 * </li>
 * <li>
 * When this {@link IngestCoordinator} is closed, any remaining data is written to partition files and a
 * {@link CompletableFuture} is returned that will complete once all of the files have been fully ingested and any
 * intermediate files removed.
 * </li>
 * </ul>
 * <p>
 * The {@link RecordBatch} and the {@link sleeper.ingest.impl.partitionfilewriter.PartitionFileWriter} to use are
 * specified using factory functions that create
 * objects which satisfy the relevant interfaces.
 *
 * @param <INCOMINGDATATYPE> The type of data that the class accepts.
 */
public class IngestCoordinator<INCOMINGDATATYPE> implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestCoordinator.class);
    private static final DecimalFormat FORMATTER = new DecimalFormat("0.#");

    private final ObjectFactory objectFactory;
    private final StateStore sleeperStateStore;
    private final Schema sleeperSchema;
    private final String sleeperIteratorClassName;
    private final String sleeperIteratorConfig;
    private final int ingestPartitionRefreshFrequencyInSeconds;
    private final RecordBatchFactory<INCOMINGDATATYPE> recordBatchFactory;
    private final PartitionFileWriterFactory partitionFileWriterFactory;
    private final IngesterIntoPartitions ingesterIntoPartitions;
    private final List<CompletableFuture<List<FileReference>>> ingestFutures;
    private final Instant ingestCoordinatorCreationTime;
    protected RecordBatch<INCOMINGDATATYPE> currentRecordBatch;
    private Instant lastPartitionsUpdateTime;
    private long recordsRead;
    private PartitionTree partitionTree;
    private boolean isClosed;

    private IngestCoordinator(Builder<INCOMINGDATATYPE> builder) {
        LOGGER.info("Creating IngestCoordinator with schema of {}", builder.schema);

        // Supplied member variables
        this.objectFactory = requireNonNull(builder.objectFactory);
        this.sleeperStateStore = requireNonNull(builder.stateStore);
        this.sleeperSchema = requireNonNull(builder.schema);
        this.sleeperIteratorClassName = builder.iteratorClassName;
        this.sleeperIteratorConfig = builder.iteratorConfig;
        this.ingestPartitionRefreshFrequencyInSeconds = builder.ingestPartitionRefreshFrequencyInSeconds;
        this.recordBatchFactory = requireNonNull(builder.recordBatchFactory);

        // Other member variables
        this.ingestCoordinatorCreationTime = Instant.now();
        this.ingestFutures = new ArrayList<>();
        this.partitionFileWriterFactory = requireNonNull(builder.partitionFileWriterFactory);
        this.ingesterIntoPartitions = new IngesterIntoPartitions(sleeperSchema,
                partitionFileWriterFactory::createPartitionFileWriter, builder.ingestFileWritingStrategy);
        this.currentRecordBatch = this.recordBatchFactory.createRecordBatch();
        this.isClosed = false;
    }

    public static Builder<?> builder() {
        return new Builder<>();
    }

    public static Builder<?> builderWith(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return builder()
                .instanceProperties(instanceProperties)
                .tableProperties(tableProperties);
    }

    /**
     * Updates the Sleeper table state store with the details of new data files which are available. This method will
     * repeatedly retry if the update fails, with an exponential backoff.
     *
     * @param sleeperStateStore The state store to update
     * @param fileReferenceList The details of the files to add to the state store
     */
    private static void updateStateStore(StateStore sleeperStateStore,
            List<FileReference> fileReferenceList) {
        boolean success = false;
        int numberOfFailures = 0;
        while (!success) {
            try {
                sleeperStateStore.addFiles(fileReferenceList);
                success = true;
            } catch (StateStoreException e) {
                LOGGER.error("Failed to update DynamoDB with new files", e);
                numberOfFailures++;
                if (numberOfFailures >= 10) {
                    throw new RetryStateStoreException("Unable to update StateStore after 10 attempts (most recent exception was " + e.getMessage() + ")", e);
                }
                // Sleep with exponential back-off
                try {
                    Thread.sleep((long) Math.pow(2.0D, numberOfFailures));
                } catch (InterruptedException e2) {
                    Thread.currentThread().interrupt();
                    throw new RetryStateStoreException("Interrupted retrying state store update after previous failure", e);
                }
            }
        }
    }

    /**
     * Commits data to the Sleeper table and state store if the current record batch is full. If the current
     * {@link RecordBatch} reports it is full, retrieve the records from the batch in sorted order, apply a Sleeper
     * iterator if required, split the sorted data into partitions and ingest the partitions into the back-end store.
     *
     * @param  isClosing                 Indicates that the {@link IngestCoordinator} is closing, so force the ingest,
     *                                   even if the record batch is not full, and do not recreate internal data
     *                                   structures.
     * @throws IOException               if there was a failure writing the new files
     * @throws IteratorCreationException if there was a failure creating the Sleeper iterator
     * @throws StateStoreException       if there was a failure adding files to the state store
     */
    private void initiateIngestIfNecessary(boolean isClosing) throws StateStoreException, IteratorCreationException, IOException {
        if (currentRecordBatch == null) {
            return;
        }
        // If the record batch is full, or it is closing, then initiate the ingest
        if (isClosing || currentRecordBatch.isFull()) {
            // Update view of partitions if necessary
            updatePartitionTreeIfNecessary();
            // Apply the Sleeper iterator to the record batch, within a try-with-resources block. This will ensure that
            // the iterators are closed in both success and failure
            try (CloseableIterator<Record> orderedRecordIteratorFromBatch = currentRecordBatch.createOrderedRecordIterator();
                    CloseableIterator<Record> recordIteratorWithSleeperIteratorApplied = new RecordIteratorWithSleeperIteratorApplied(
                            objectFactory,
                            sleeperSchema,
                            sleeperIteratorClassName,
                            sleeperIteratorConfig,
                            orderedRecordIteratorFromBatch)) {
                // Create a future which completes once the partitions are created, the records ingested
                // and the state store updated.
                // Note that once initiateIngest() has been called, below, the record batch has been consumed and is no
                // longer required.
                CompletableFuture<List<FileReference>> consumedFuture = ingesterIntoPartitions
                        .initiateIngest(recordIteratorWithSleeperIteratorApplied, partitionTree)
                        .thenApply(fileReferenceList -> {
                            updateStateStore(sleeperStateStore, fileReferenceList);
                            return fileReferenceList;
                        });
                ingestFutures.add(consumedFuture);
            }
            // The record batch has now been consumed and so close it.
            currentRecordBatch.close();
            currentRecordBatch = (isClosing) ? null : recordBatchFactory.createRecordBatch();
        }
    }

    /**
     * Retrieves the partition tree from the state store if the current view is out of date. If too much time has
     * elapsed since the last refresh, it queries the {@link StateStore} to retrieve the current partition tree.
     *
     * @throws StateStoreException if there was a failure reading partitions from the state store
     */
    private void updatePartitionTreeIfNecessary() throws StateStoreException {
        if (lastPartitionsUpdateTime == null) {
            LOGGER.info("Updating list of leaf partitions for the first time");
        } else {
            LoggedDuration duration = LoggedDuration.withFullOutput(lastPartitionsUpdateTime, Instant.now());
            if (duration.getSeconds() > ingestPartitionRefreshFrequencyInSeconds) {
                LOGGER.info("Updating list of leaf partitions as {} since last updated", duration);
            } else {
                LOGGER.info("Not updating list of leaf partitions as refresh frequency of {} seconds not reached",
                        ingestPartitionRefreshFrequencyInSeconds);
                return;
            }
        }

        LOGGER.debug("Loading partitions from state store {}", sleeperStateStore);
        List<Partition> allPartitions = sleeperStateStore.getAllPartitions();
        partitionTree = new PartitionTree(allPartitions);
        lastPartitionsUpdateTime = Instant.now();
        LOGGER.info("There are {} partitions", allPartitions.size());
    }

    /**
     * Close this ingester synchronously. This is required by {@link AutoCloseable}. This is the only closing method
     * that may be called more than once. Second and subsequent calls are ignored and a warning is logged.
     * <p>
     * This method uses {@link #asyncCloseReturningResult()} and waits for that future to complete before
     * returning.
     *
     * @throws IOException               if there was a failure writing the new files
     * @throws IteratorCreationException if there was a failure creating the Sleeper iterator
     * @throws StateStoreException       if there was a failure adding files to the state store
     */
    @Override
    public void close() throws StateStoreException, IteratorCreationException, IOException {
        if (isClosed) {
            LOGGER.warn("Closing an IngestCoordinator that has already been closed");
        } else {
            asyncCloseReturningResult().join();
        }
    }

    /**
     * Close this ingester synchronously, returning information about every file that was ingested and added to the
     * state store. This method uses {@link #asyncCloseReturningResult()} and waits for that future to complete
     * before returning.
     *
     * @return                           Details about every file that was added to the state store.
     * @throws IOException               if there was a failure writing the new files
     * @throws IteratorCreationException if there was a failure creating the Sleeper iterator
     * @throws StateStoreException       if there was a failure adding files to the state store
     */
    public IngestResult closeReturningResult() throws StateStoreException, IteratorCreationException, IOException {
        return asyncCloseReturningResult().join();
    }

    /**
     * Close this ingester asynchronously, returning information about every partition file that was ingested and added
     * to the state store. Closing the ingester causes any data that has been ingested to be sorted, partitioned and
     * uploaded to the back-end storage, and the state store to be updated. The returned {@link CompletableFuture} will
     * only resolve once this is complete and all intermediate resources have been freed.
     *
     * @return                           A {@link CompletableFuture} which resolves to a list of information about every
     *                                   file that was added to the state store.
     * @throws IOException               if there was a failure writing the new files
     * @throws IteratorCreationException if there was a failure creating the Sleeper iterator
     * @throws StateStoreException       if there was a failure adding files to the state store
     */
    public CompletableFuture<IngestResult> asyncCloseReturningResult() throws StateStoreException, IteratorCreationException, IOException {
        if (isClosed) {
            throw new AssertionError("Attempt to close IngestCoordinator and return results twice");
        }
        isClosed = true;
        // Ingest any data remaining in the current RecordBatch
        initiateIngestIfNecessary(true);
        // There are many futures which have been created. Create a future which waits for them all to complete
        // and then returns a flattened list of all of the FileReference objects which were passed to the state store
        return CompletableFuture.allOf(ingestFutures.toArray(new CompletableFuture[0]))
                .whenComplete((msg, ex) -> internalClose())
                .thenApply(dummy -> {
                    List<FileReference> filesWritten = ingestFutures.stream().map(CompletableFuture::join)
                            .flatMap(List::stream).collect(Collectors.toList());
                    IngestResult result = IngestResult.fromReadAndWritten(recordsRead, filesWritten);
                    long noOfRecordsWritten = result.getRecordsWritten();
                    LoggedDuration duration = LoggedDuration.withFullOutput(ingestCoordinatorCreationTime, Instant.now());
                    METRICS_LOGGER.info("Wrote {} records to S3 in {} at {} per second",
                            noOfRecordsWritten,
                            duration,
                            FORMATTER.format(noOfRecordsWritten / (double) duration.getSeconds()));
                    return result;
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
        try {
            partitionFileWriterFactory.close();
        } catch (Exception e) {
            LOGGER.error("Failed to close partitionFileWriterFactory", e);
        }
        try {
            recordBatchFactory.close();
        } catch (Exception e) {
            LOGGER.error("Failed to close recordBatchFactory", e);
        }
    }

    /**
     * Write data to this ingester. When this method is called, it may initiate significant activity such as sorting the
     * data that is held in memory and flushing it to local disk, or merging local files and saving them as partition
     * files on a remote file store. The amount of time taken by a call to this function varies significantly.
     *
     * @param  data                      the data to ingest
     * @throws IOException               if there was a failure writing the new files
     * @throws IteratorCreationException if there was a failure creating the Sleeper iterator
     * @throws StateStoreException       if there was a failure adding files to the state store
     */
    public void write(INCOMINGDATATYPE data) throws IOException, IteratorCreationException, StateStoreException {
        try {
            initiateIngestIfNecessary(false);
            currentRecordBatch.append(data);
            recordsRead++;
        } catch (Exception e) {
            internalClose();
            throw e;
        }
    }

    public static class Builder<T> {
        private ObjectFactory objectFactory;
        private StateStore stateStore;
        private Schema schema;
        private String iteratorClassName;
        private String iteratorConfig;
        private int ingestPartitionRefreshFrequencyInSeconds;
        private RecordBatchFactory<T> recordBatchFactory;
        private PartitionFileWriterFactory partitionFileWriterFactory;
        private IngestFileWritingStrategy ingestFileWritingStrategy = IngestFileWritingStrategy.ONE_FILE_PER_LEAF;

        Builder() {
        }

        /**
         * The factory to use to create Sleeper iterators.
         *
         * @param  objectFactory the object factory
         * @return               the builder for call chaining
         */
        public Builder<T> objectFactory(ObjectFactory objectFactory) {
            this.objectFactory = objectFactory;
            return this;
        }

        /**
         * The Sleeper state store.
         *
         * @param  stateStore the state store
         * @return            the builder for call chaining
         */
        public Builder<T> stateStore(StateStore stateStore) {
            this.stateStore = stateStore;
            return this;
        }

        /**
         * The Sleeper schema of the data.
         *
         * @param  schema the schema
         * @return        the builder for call chaining
         */
        public Builder<T> schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        /**
         * The Sleeper iterator class name.
         *
         * @param  iteratorClassName the class name
         * @return                   the builder for call chaining
         */
        public Builder<T> iteratorClassName(String iteratorClassName) {
            this.iteratorClassName = iteratorClassName;
            return this;
        }

        /**
         * The Sleeper iterator configuration.
         *
         * @param  iteratorConfig the configuration
         * @return                the builder for call chaining
         */
        public Builder<T> iteratorConfig(String iteratorConfig) {
            this.iteratorConfig = iteratorConfig;
            return this;
        }

        /**
         * The number of seconds to wait before the current list of partitions is refreshed from the state store.
         *
         * @param  ingestPartitionRefreshFrequencyInSeconds the wait time
         * @return                                          the builder for call chaining
         */
        public Builder<T> ingestPartitionRefreshFrequencyInSeconds(int ingestPartitionRefreshFrequencyInSeconds) {
            this.ingestPartitionRefreshFrequencyInSeconds = ingestPartitionRefreshFrequencyInSeconds;
            return this;
        }

        /**
         * A factory to create new record batches.
         *
         * @param  recordBatchFactory the factory
         * @return                    the builder for call chaining
         */
        public <R> Builder<R> recordBatchFactory(RecordBatchFactory<R> recordBatchFactory) {
            this.recordBatchFactory = (RecordBatchFactory<T>) recordBatchFactory;
            return (Builder<R>) this;
        }

        /**
         * A factory to create new writers to write a file in a partition.
         *
         * @param  partitionFileWriterFactory the factory
         * @return                            the builder for call chaining
         */
        public Builder<T> partitionFileWriterFactory(PartitionFileWriterFactory partitionFileWriterFactory) {
            this.partitionFileWriterFactory = partitionFileWriterFactory;
            return this;
        }

        /**
         * Determines how to create new files while performing an ingest. Defaults to
         * {@link IngestFileWritingStrategy#ONE_FILE_PER_LEAF}.
         *
         * @param  ingestFileWritingStrategy the mode for ingesting files.
         * @return                           the builder for call chaining.
         */
        public Builder<T> ingestFileWritingStrategy(IngestFileWritingStrategy ingestFileWritingStrategy) {
            this.ingestFileWritingStrategy = ingestFileWritingStrategy;
            return this;
        }

        public Builder<T> instanceProperties(InstanceProperties instanceProperties) {
            return ingestPartitionRefreshFrequencyInSeconds(
                    instanceProperties.getInt(INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS));
        }

        public Builder<T> tableProperties(TableProperties tableProperties) {
            return schema(tableProperties.getSchema())
                    .iteratorClassName(tableProperties.get(ITERATOR_CLASS_NAME))
                    .iteratorConfig(tableProperties.get(ITERATOR_CONFIG))
                    .ingestFileWritingStrategy(tableProperties.getEnumValue(INGEST_FILE_WRITING_STRATEGY, IngestFileWritingStrategy.class));
        }

        public IngestCoordinator<T> build() {
            return new IngestCoordinator<>(this);
        }
    }
}
