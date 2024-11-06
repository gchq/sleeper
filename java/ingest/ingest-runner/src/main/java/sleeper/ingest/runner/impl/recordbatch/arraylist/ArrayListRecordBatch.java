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
package sleeper.ingest.runner.impl.recordbatch.arraylist;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.MergingIterator;
import sleeper.core.record.Record;
import sleeper.core.record.RecordComparator;
import sleeper.core.schema.Schema;
import sleeper.core.util.LoggedDuration;
import sleeper.ingest.runner.impl.ParquetConfiguration;
import sleeper.ingest.runner.impl.recordbatch.RecordBatch;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;

import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Stores a batch of records in an array in memory. This class implements a {@link RecordBatch} backed by an ArrayList,
 * and spilled to local disk as Parquet files when the ArrayList contains a set number of records. Each time the records
 * are spilled to disk, they are sorted.
 * <p>
 * When the batch is read, all of the sorted files and the sorted in-memory batch are merged together into a single
 * iterator of sorted records.
 * <p>
 * The batch is considered to be full when the local disk contains more than a specified number of records.
 * <p>
 * This class needs a mapper extending the {@link ArrayListRecordMapper} interface. Data is always retrieved from
 * this batch as @link Record} objects and the mapper is responsible for any type conversion.
 *
 * @param <INCOMINGDATATYPE> The type of data that can be added to this batch.
 */
public class ArrayListRecordBatch<INCOMINGDATATYPE> implements RecordBatch<INCOMINGDATATYPE> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArrayListRecordBatch.class);
    private static final DecimalFormat FORMATTER = new DecimalFormat("0.#");
    private final ParquetConfiguration parquetConfiguration;
    private final Schema sleeperSchema;
    private final ArrayListRecordMapper<INCOMINGDATATYPE> recordMapper;
    private final String localWorkingDirectory;
    private final int maxNoOfRecordsInMemory;
    private final long maxNoOfRecordsInLocalStore;
    private final Configuration hadoopConfiguration;
    private final UUID uniqueIdentifier;
    private final List<Record> inMemoryBatch;
    private final List<String> localFileNames;
    private long noOfRecordsInLocalStore;
    private CloseableIterator<Record> internalOrderedRecordIterator;
    private boolean isWriteable;
    private int batchNo;

    /**
     * Create an instance. Should be called by an {@link ArrayListRecordBatchFactory}.
     *
     * @param parquetConfiguration       Hadoop, schema and Parquet configuration for writing files.
     *                                   The Hadoop configuration is used during read and write of the Parquet files.
     *                                   Note that the library code uses caching and so unusual errors can occur if
     *                                   different configurations are used in different calls.
     * @param localWorkingDirectory      A local directory to use to store temporary files
     * @param maxNoOfRecordsInMemory     The maximum number of records to store in the internal ArrayList
     * @param maxNoOfRecordsInLocalStore The maximum number of records to store on the local disk
     */
    public ArrayListRecordBatch(ParquetConfiguration parquetConfiguration,
            ArrayListRecordMapper<INCOMINGDATATYPE> recordMapper,
            String localWorkingDirectory,
            int maxNoOfRecordsInMemory,
            long maxNoOfRecordsInLocalStore) {
        this.parquetConfiguration = requireNonNull(parquetConfiguration);
        this.sleeperSchema = parquetConfiguration.getTableProperties().getSchema();
        this.recordMapper = recordMapper;
        this.localWorkingDirectory = requireNonNull(localWorkingDirectory);
        this.maxNoOfRecordsInMemory = maxNoOfRecordsInMemory;
        this.maxNoOfRecordsInLocalStore = maxNoOfRecordsInLocalStore;
        this.hadoopConfiguration = parquetConfiguration.getHadoopConfiguration();
        this.uniqueIdentifier = UUID.randomUUID();
        this.internalOrderedRecordIterator = null;
        this.isWriteable = true;
        this.inMemoryBatch = new ArrayList<>(maxNoOfRecordsInMemory);
        this.noOfRecordsInLocalStore = 0L;
        this.batchNo = 0;
        this.localFileNames = new ArrayList<>();
    }

    /**
     * Internal method to add a record to the internal batch, flushing to local disk first if necessary.
     *
     * @param  record      the record to add to the batch
     * @throws IOException if there was a failure writing the local file
     */
    protected void addRecordToBatch(Record record) throws IOException {
        if (!isWriteable) {
            throw new AssertionError("Attempt to write to a batch where an iterator has already been created");
        }
        if (inMemoryBatch.size() >= maxNoOfRecordsInMemory) {
            flushToLocalDiskAndClear();
        }
        inMemoryBatch.add(record);
    }

    /**
     * Flushes the in-memory batch of records to a local file and then clears the in-memory batch.
     *
     * @throws IOException if there was a failure writing the local file
     */
    private void flushToLocalDiskAndClear() throws IOException {
        if (inMemoryBatch.isEmpty()) {
            LOGGER.info("There are no records to flush");
        } else {
            Instant startTime = Instant.now();
            String outputFileName = String.format("%s/localfile-batch-%s-file-%09d.parquet",
                    localWorkingDirectory,
                    uniqueIdentifier,
                    batchNo);
            inMemoryBatch.sort(new RecordComparator(sleeperSchema));
            Instant writeTime = Instant.now();
            // Write the records to a local Parquet file. The try-with-resources block ensures that the writer
            // is closed in both success and failure.
            try (ParquetWriter<Record> parquetWriter = parquetConfiguration.createParquetWriter(outputFileName)) {
                for (Record record : inMemoryBatch) {
                    parquetWriter.write(record);
                }
            }
            Instant finishTime = Instant.now();
            LoggedDuration wholeDuration = LoggedDuration.withShortOutput(startTime, finishTime);
            LoggedDuration sortDuration = LoggedDuration.withShortOutput(startTime, writeTime);
            LoggedDuration writeDuration = LoggedDuration.withShortOutput(writeTime, finishTime);
            LOGGER.info("Wrote {} records to local file in {} ({}/s) " +
                    "[sorting {} ({}/s), writing {} ({}/s)] - filename: {}",
                    inMemoryBatch.size(),
                    wholeDuration,
                    FORMATTER.format(inMemoryBatch.size() / (double) wholeDuration.getSeconds()),
                    sortDuration,
                    FORMATTER.format(inMemoryBatch.size() / (double) sortDuration.getSeconds()),
                    writeDuration,
                    FORMATTER.format(inMemoryBatch.size() / (double) writeDuration.getSeconds()),
                    outputFileName);
            localFileNames.add(outputFileName);
            noOfRecordsInLocalStore += inMemoryBatch.size();
        }
        batchNo++;
        inMemoryBatch.clear();
    }

    @Override
    public void append(INCOMINGDATATYPE data) throws IOException {
        addRecordToBatch(recordMapper.map(data));
    }

    /**
     * Indicates whether the batch is full. This is considered full when the in-memory batch is full and when, if it
     * were written to disk, the total number of records on disk would exceed the limit specified during construction.
     *
     * @return a flag indicating whether or not the batch is full
     */
    @Override
    public boolean isFull() {
        return inMemoryBatch.size() >= maxNoOfRecordsInMemory &&
                (noOfRecordsInLocalStore + inMemoryBatch.size()) >= maxNoOfRecordsInLocalStore;
    }

    /**
     * Merge-sort the sorted files on the local disk and records in memory into one iterator. Note that this method may
     * only be called once.
     *
     * @return             an iterator of the sorted records
     * @throws IOException if there was a failure writing the local file
     */
    @Override
    public CloseableIterator<Record> createOrderedRecordIterator() throws IOException {
        if (!isWriteable || (internalOrderedRecordIterator != null)) {
            throw new AssertionError("Attempt to create an iterator where an iterator has already been created");
        }
        isWriteable = false;
        // Flush the current in-memory batch to disk, to free up as much memory as possible for the merge
        flushToLocalDiskAndClear();
        // Create an iterator for each one of the local Parquet files
        List<CloseableIterator<Record>> inputIterators = new ArrayList<>();
        try {
            for (String localFileName : localFileNames) {
                ParquetReader<Record> readerForBatch = createParquetReader(localFileName);
                ParquetReaderIterator recordIterator = new ParquetReaderIterator(readerForBatch);
                inputIterators.add(recordIterator);
                LOGGER.info("Created reader for file {}", localFileName);
            }
        } catch (Exception e1) {
            // Clean up the iterators that have been created
            // Rely on the caller to call close() to delete local files
            inputIterators.forEach(inputIterator -> {
                try {
                    inputIterator.close();
                } catch (Exception e2) {
                    LOGGER.error("Error closing iterator", e2);
                }
            });
            throw e1;
        }
        // Merge into one sorted iterator
        internalOrderedRecordIterator = new MergingIterator(sleeperSchema, inputIterators);
        return internalOrderedRecordIterator;
    }

    /**
     * Close this batch, remove all local files and free resources.
     */
    @Override
    public void close() {
        deleteAllLocalFiles();
        try {
            internalOrderedRecordIterator.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a reader for a local Parquet file. Uses the parameters specified during construction.
     *
     * @param  inputFile   the Parquet file to read
     * @return             the {@link ParquetReader}
     * @throws IOException Thrown when the reader cannot be created
     */
    private ParquetReader<Record> createParquetReader(String inputFile) throws IOException {
        ParquetReader.Builder<Record> builder = new ParquetRecordReader.Builder(new Path(inputFile), sleeperSchema)
                .withConf(hadoopConfiguration);
        return builder.build();
    }

    /**
     * Delete all of the local files. Errors are logged but are not propagated.
     */
    private void deleteAllLocalFiles() {
        if (!localFileNames.isEmpty()) {
            LOGGER.info("Deleting {} local batch files, first: {} last: {}",
                    localFileNames.size(),
                    localFileNames.get(0),
                    localFileNames.get(localFileNames.size() - 1));
            localFileNames.forEach(localFileName -> {
                try {
                    boolean success = FileSystem.getLocal(hadoopConfiguration).delete(new Path(localFileName), false);
                    if (!success) {
                        LOGGER.error("Failed to delete local file {}", localFileName);
                    }
                } catch (IOException e) {
                    LOGGER.error("Failed to delete local file " + localFileName, e);
                }
            });
        }
        localFileNames.clear();
    }

}
