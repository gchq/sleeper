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
package sleeper.ingest.runner.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.validation.IngestFileWritingStrategy;
import sleeper.core.range.Range;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.ingest.runner.impl.partitionfilewriter.PartitionFileWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Writes records for a partition into Sleeper data files. The records must have been sorted before they are passed to
 * this class. The actual write is performed by {@link PartitionFileWriter} classes and a factory function
 * to generate these is provided when this class is constructed.
 */
class IngesterIntoPartitions {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngesterIntoPartitions.class);

    private final Function<Partition, PartitionFileWriter> partitionFileWriterFactoryFn;
    private final Schema sleeperSchema;
    private final IngestFileWritingStrategy ingestFileWritingStrategy;

    /**
     * Create an instance.
     *
     * @param sleeperSchema                the Sleeper schema
     * @param partitionFileWriterFactoryFn a function which takes a {@link Partition} and returns the
     *                                     {@link PartitionFileWriter} which will write {@link Record} objects to that
     *                                     partition
     * @param ingestFileWritingStrategy    how files and references should be created during ingest
     */
    IngesterIntoPartitions(
            Schema sleeperSchema,
            Function<Partition, PartitionFileWriter> partitionFileWriterFactoryFn,
            IngestFileWritingStrategy ingestFileWritingStrategy) {
        this.partitionFileWriterFactoryFn = requireNonNull(partitionFileWriterFactoryFn);
        this.sleeperSchema = requireNonNull(sleeperSchema);
        this.ingestFileWritingStrategy = requireNonNull(ingestFileWritingStrategy);
    }

    /**
     * Close several file writers at once, rethrowing any errors as unchecked exceptions.
     *
     * @param  partitionFileWriters the {@link PartitionFileWriter} objects to close
     * @return                      the {@link CompletableFuture} objects corresponding to the closed
     *                              {@link PartitionFileWriter} objects, in the same order
     * @throws IOException          may contain multiple suppressed exceptions, as each {@link PartitionFileWriter}
     *                              may fail as it is closed
     */
    private static List<CompletableFuture<FileReference>> closeMultiplePartitionFileWriters(
            Collection<PartitionFileWriter> partitionFileWriters) throws IOException {
        List<Exception> exceptionList = new ArrayList<>();
        List<CompletableFuture<FileReference>> futures = partitionFileWriters.stream()
                .map(partitionFileWriter -> {
                    try {
                        return partitionFileWriter.close();
                    } catch (Exception e) {
                        exceptionList.add(e);
                        return CompletableFuture.<FileReference>completedFuture(null);
                    }
                }).collect(Collectors.toList());
        if (exceptionList.isEmpty()) {
            return futures;
        } else {
            IOException aggregateIOException = new IOException();
            exceptionList.forEach(aggregateIOException::addSuppressed);
            throw aggregateIOException;
        }
    }

    /**
     * Initiate the ingest of records passed as an iterator. The records must be supplied in sort-order. When this
     * method returns, all of the records will have been read from the iterator and the iterator may be discarded by the
     * caller.
     *
     * @param  orderedRecordIterator the {@link Record} objects to write, passed in sort order
     * @param  partitionTree         the {@link PartitionTree} to used to determine which partition to place each record
     *                               in
     * @return                       a {@link CompletableFuture} which completes to return a list of
     *                               {@link FileReference} objects, one for each partition file that has been created
     * @throws IOException           if there was a failure writing the file
     */
    public CompletableFuture<List<FileReference>> initiateIngest(
            CloseableIterator<Record> orderedRecordIterator, PartitionTree partitionTree) throws IOException {
        if (ingestFileWritingStrategy == IngestFileWritingStrategy.ONE_FILE_PER_LEAF) {
            return ingestOneFilePerLeafPartition(orderedRecordIterator, partitionTree);
        } else if (ingestFileWritingStrategy == IngestFileWritingStrategy.ONE_REFERENCE_PER_LEAF) {
            return ingestOneFileWithReferencesInLeafPartitions(orderedRecordIterator, partitionTree);
        } else {
            throw new IllegalArgumentException("Unknown ingest file writing strategy: " + ingestFileWritingStrategy);
        }
    }

    public CompletableFuture<List<FileReference>> ingestOneFilePerLeafPartition(
            CloseableIterator<Record> orderedRecordIterator, PartitionTree partitionTree) throws IOException {
        List<String> rowKeyNames = sleeperSchema.getRowKeyFieldNames();
        String firstDimensionRowKey = rowKeyNames.get(0);
        Map<String, PartitionFileWriter> partitionIdToFileWriterMap = new HashMap<>();
        Range currentFirstDimensionRange = null;
        // Set up various flags, counters and the like which will be updated as the write progresses
        Partition currentPartition = null;
        PartitionFileWriter currentPartitionFileWriter = null;
        // Prepare arrays to hold the results
        List<CompletableFuture<FileReference>> completableFutures = new ArrayList<>();

        // Log and return if the iterator is empty
        if (!orderedRecordIterator.hasNext()) {
            LOGGER.info("There are no records");
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        // Loop through the iterator, creating new partition files whenever this is required.
        // The records are in sort-order and this means that all of the partitions which share the same
        // first dimension range are created at once, and then they may be closed as soon as the records no
        // longer sit inside that first dimension range.
        try {
            while (orderedRecordIterator.hasNext()) {
                Record record = orderedRecordIterator.next();
                Key key = Key.create(record.getValues(rowKeyNames));
                // Ensure that the current partition is the correct one for the new record
                if (currentPartition == null || !currentPartition.isRowKeyInPartition(sleeperSchema, key)) {
                    // Close all of the current partition file writers if the first dimension has changed.
                    if (currentFirstDimensionRange != null &&
                            !currentFirstDimensionRange.doesRangeContainObject(record.get(firstDimensionRowKey))) {
                        completableFutures.addAll(closeMultiplePartitionFileWriters(partitionIdToFileWriterMap.values()));
                        partitionIdToFileWriterMap.clear();
                    }
                    currentPartition = partitionTree.getLeafPartition(sleeperSchema, key);
                    currentFirstDimensionRange = currentPartition.getRegion().getRange(firstDimensionRowKey);
                    // Create a new partition file writer if required
                    if (!partitionIdToFileWriterMap.containsKey(currentPartition.getId())) {
                        partitionIdToFileWriterMap.put(currentPartition.getId(), partitionFileWriterFactoryFn.apply(currentPartition));
                    }
                    currentPartitionFileWriter = partitionIdToFileWriterMap.get(currentPartition.getId());
                }
                // Write records to the current partition file writer
                currentPartitionFileWriter.append(record);
            }
            completableFutures.addAll(closeMultiplePartitionFileWriters(partitionIdToFileWriterMap.values()));
        } catch (Exception e) {
            partitionIdToFileWriterMap.values().forEach(PartitionFileWriter::abort);
            throw e;
        }

        // Create a future where all of the partitions have finished uploading and then return the FileReference
        // objects as a list
        return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]))
                .thenApply(dummy -> completableFutures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));
    }

    public CompletableFuture<List<FileReference>> ingestOneFileWithReferencesInLeafPartitions(
            CloseableIterator<Record> orderedRecordIterator, PartitionTree partitionTree) throws IOException {
        List<String> rowKeyNames = sleeperSchema.getRowKeyFieldNames();
        Map<String, Long> partitionIdToRecordCount = new HashMap<>();
        Partition currentPartition = null;
        PartitionFileWriter rootFileWriter = partitionFileWriterFactoryFn.apply(partitionTree.getRootPartition());
        try {
            while (orderedRecordIterator.hasNext()) {
                Record record = orderedRecordIterator.next();
                rootFileWriter.append(record);
                Key key = Key.create(record.getValues(rowKeyNames));
                if (currentPartition == null || !currentPartition.isRowKeyInPartition(sleeperSchema, key)) {
                    currentPartition = partitionTree.getLeafPartition(sleeperSchema, key);
                }
                partitionIdToRecordCount.compute(currentPartition.getId(),
                        (partitionId, recordCount) -> (recordCount == null) ? 1 : recordCount + 1);
            }
        } catch (Exception e) {
            rootFileWriter.abort();
            throw e;
        }
        boolean hasOnePartition = partitionIdToRecordCount.keySet().size() == 1;
        return rootFileWriter.close().thenApply(rootFile -> partitionIdToRecordCount.entrySet().stream()
                .map((entry) -> FileReference.builder()
                        .partitionId(entry.getKey())
                        .filename(rootFile.getFilename())
                        .numberOfRecords(entry.getValue())
                        .countApproximate(false)
                        .onlyContainsDataForThisPartition(hasOnePartition)
                        .build())
                .collect(Collectors.toList()));
    }
}
