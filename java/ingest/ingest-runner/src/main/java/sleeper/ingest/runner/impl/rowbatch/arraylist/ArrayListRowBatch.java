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
package sleeper.ingest.runner.impl.rowbatch.arraylist;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.MergingIterator;
import sleeper.core.row.Row;
import sleeper.core.row.RowComparator;
import sleeper.core.rowbatch.RowBatch;
import sleeper.core.schema.Schema;
import sleeper.core.util.LoggedDuration;
import sleeper.ingest.runner.impl.ParquetConfiguration;
import sleeper.parquet.row.ParquetReaderIterator;
import sleeper.parquet.row.ParquetRowReaderFactory;

import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Stores a batch of rows in an array in memory. This class implements a {@link RowBatch} backed by an ArrayList,
 * and spilled to local disk as Parquet files when the ArrayList contains a set number of rows. Each time the rows
 * are spilled to disk, they are sorted.
 * <p>
 * When the batch is read, all of the sorted files and the sorted in-memory batch are merged together into a single
 * iterator of sorted rows.
 * <p>
 * The batch is considered to be full when the local disk contains more than a specified number of rows.
 * <p>
 * This class needs a mapper extending the {@link ArrayListRowMapper} interface. Data is always retrieved from
 * this batch as {@link Row} objects and the mapper is responsible for any type conversion.
 *
 * @param <INCOMINGDATATYPE> The type of data that can be added to this batch.
 */
public class ArrayListRowBatch<INCOMINGDATATYPE> implements RowBatch<INCOMINGDATATYPE> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArrayListRowBatch.class);
    private static final DecimalFormat FORMATTER = new DecimalFormat("0.#");
    private final ParquetConfiguration parquetConfiguration;
    private final Schema sleeperSchema;
    private final ArrayListRowMapper<INCOMINGDATATYPE> rowMapper;
    private final String localWorkingDirectory;
    private final int maxNoOfRowsInMemory;
    private final long maxNoOfRowsInLocalStore;
    private final Configuration hadoopConfiguration;
    private final UUID uniqueIdentifier;
    private final List<Row> inMemoryBatch;
    private final List<String> localFileNames;
    private long noOfRowsInLocalStore;
    private CloseableIterator<Row> internalOrderedRowIterator;
    private boolean isWriteable;
    private int batchNo;

    /**
     * Create an instance. Should be called by an {@link ArrayListRowBatchFactory}.
     *
     * @param parquetConfiguration    Hadoop, schema and Parquet configuration for writing files.
     *                                The Hadoop configuration is used during read and write of the Parquet files.
     *                                Note that the library code uses caching and so unusual errors can occur if
     *                                different configurations are used in different calls.
     * @param rowMapper               a mapper to convert from the incoming data type to Sleeper rows
     * @param localWorkingDirectory   a local directory to use to store temporary files
     * @param maxNoOfRowsInMemory     the maximum number of rows to store in the internal ArrayList
     * @param maxNoOfRowsInLocalStore the maximum number of rows to store on the local disk
     */
    public ArrayListRowBatch(ParquetConfiguration parquetConfiguration,
            ArrayListRowMapper<INCOMINGDATATYPE> rowMapper,
            String localWorkingDirectory,
            int maxNoOfRowsInMemory,
            long maxNoOfRowsInLocalStore) {
        this.parquetConfiguration = requireNonNull(parquetConfiguration);
        this.sleeperSchema = parquetConfiguration.getTableProperties().getSchema();
        this.rowMapper = rowMapper;
        this.localWorkingDirectory = requireNonNull(localWorkingDirectory);
        this.maxNoOfRowsInMemory = maxNoOfRowsInMemory;
        this.maxNoOfRowsInLocalStore = maxNoOfRowsInLocalStore;
        this.hadoopConfiguration = parquetConfiguration.getHadoopConfiguration();
        this.uniqueIdentifier = UUID.randomUUID();
        this.internalOrderedRowIterator = null;
        this.isWriteable = true;
        this.inMemoryBatch = new ArrayList<>(maxNoOfRowsInMemory);
        this.noOfRowsInLocalStore = 0L;
        this.batchNo = 0;
        this.localFileNames = new ArrayList<>();
    }

    /**
     * Internal method to add a row to the internal batch, flushing to local disk first if necessary.
     *
     * @param  row         the row to add to the batch
     * @throws IOException if there was a failure writing the local file
     */
    protected void addRowToBatch(Row row) throws IOException {
        if (!isWriteable) {
            throw new AssertionError("Attempt to write to a batch where an iterator has already been created");
        }
        if (inMemoryBatch.size() >= maxNoOfRowsInMemory) {
            flushToLocalDiskAndClear();
        }
        inMemoryBatch.add(row);
    }

    /**
     * Flushes the in-memory batch of rows to a local file and then clears the in-memory batch.
     *
     * @throws IOException if there was a failure writing the local file
     */
    private void flushToLocalDiskAndClear() throws IOException {
        if (inMemoryBatch.isEmpty()) {
            LOGGER.info("There are no rows to flush");
        } else {
            Instant startTime = Instant.now();
            String outputFileName = String.format("%s/localfile-batch-%s-file-%09d.parquet",
                    localWorkingDirectory,
                    uniqueIdentifier,
                    batchNo);
            inMemoryBatch.sort(new RowComparator(sleeperSchema));
            Instant writeTime = Instant.now();
            // Write the rows to a local Parquet file. The try-with-resources block ensures that the writer
            // is closed in both success and failure.
            try (ParquetWriter<Row> parquetWriter = parquetConfiguration.createParquetWriter(outputFileName)) {
                for (Row row : inMemoryBatch) {
                    parquetWriter.write(row);
                }
            }
            Instant finishTime = Instant.now();
            LoggedDuration wholeDuration = LoggedDuration.withShortOutput(startTime, finishTime);
            LoggedDuration sortDuration = LoggedDuration.withShortOutput(startTime, writeTime);
            LoggedDuration writeDuration = LoggedDuration.withShortOutput(writeTime, finishTime);
            LOGGER.info("Wrote {} rows to local file in {} ({}/s) " +
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
            noOfRowsInLocalStore += inMemoryBatch.size();
        }
        batchNo++;
        inMemoryBatch.clear();
    }

    @Override
    public void append(INCOMINGDATATYPE data) throws IOException {
        addRowToBatch(rowMapper.map(data));
    }

    /**
     * Indicates whether the batch is full. This is considered full when the in-memory batch is full and when, if it
     * were written to disk, the total number of rows on disk would exceed the limit specified during construction.
     *
     * @return a flag indicating whether or not the batch is full
     */
    @Override
    public boolean isFull() {
        return inMemoryBatch.size() >= maxNoOfRowsInMemory &&
                (noOfRowsInLocalStore + inMemoryBatch.size()) >= maxNoOfRowsInLocalStore;
    }

    /**
     * Merge-sort the sorted files on the local disk and rows in memory into one iterator. Note that this method may
     * only be called once.
     *
     * @return             an iterator of the sorted rows
     * @throws IOException if there was a failure writing the local file
     */
    @Override
    public CloseableIterator<Row> createOrderedRowIterator() throws IOException {
        if (!isWriteable || internalOrderedRowIterator != null) {
            throw new AssertionError("Attempt to create an iterator where an iterator has already been created");
        }
        isWriteable = false;
        // Flush the current in-memory batch to disk, to free up as much memory as possible for the merge
        flushToLocalDiskAndClear();
        // Create an iterator for each one of the local Parquet files
        List<CloseableIterator<Row>> inputIterators = new ArrayList<>();
        try {
            for (String localFileName : localFileNames) {
                ParquetReader<Row> readerForBatch = createParquetReader(localFileName);
                ParquetReaderIterator rowIterator = new ParquetReaderIterator(readerForBatch);
                inputIterators.add(rowIterator);
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
        internalOrderedRowIterator = new MergingIterator(sleeperSchema, inputIterators);
        return internalOrderedRowIterator;
    }

    /**
     * Close this batch, remove all local files and free resources.
     */
    @Override
    public void close() {
        deleteAllLocalFiles();
        try {
            internalOrderedRowIterator.close();
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
    private ParquetReader<Row> createParquetReader(String inputFile) throws IOException {
        ParquetReader.Builder<Row> builder = ParquetRowReaderFactory.parquetRowReaderBuilder(new Path(inputFile), sleeperSchema)
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
