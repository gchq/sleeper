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
package sleeper.trino;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.SchemaTableName;

import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.trino.handle.SleeperColumnHandle;
import sleeper.trino.remotesleeperconnection.SleeperConnectionAsTrino;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Provides a way to write rows into Sleeper. Used when an INSERT statement is run.
 * <p>
 * This class currently references the Sleeper-internal {@link IngestCoordinator} class. It may be a good idea to reduce
 * the spread of internal classes like this so that the Sleeper-specific code all lives within
 * {@link SleeperConnectionAsTrino}.
 * <p>
 * The error handling is limited within this class.
 */
public class SleeperPageSink implements ConnectorPageSink {
    private static final Logger LOGGER = Logger.get(SleeperPageSink.class);

    private final IngestCoordinator<Page> ingestRecordsPageAsync;
    private final List<SleeperColumnHandle> sleeperColumnHandlesInOrder;
    private long noOfAppends = 0L;
    private long noOfPositionsWritten = 0L;

    /**
     * Construct the page sink.
     *
     * @param sleeperConnectionAsTrino    the connection to Sleeper
     * @param schemaTableName             the table to append rows to
     * @param sleeperColumnHandlesInOrder the columns which are to be appended (must currently be all of the columns in
     *                                    the table)
     */
    public SleeperPageSink(SleeperConnectionAsTrino sleeperConnectionAsTrino,
            SchemaTableName schemaTableName,
            List<SleeperColumnHandle> sleeperColumnHandlesInOrder) {
        LOGGER.debug("Creating a SleeperPageSink for %s", schemaTableName.getTableName());
        this.ingestRecordsPageAsync = sleeperConnectionAsTrino.createIngestCoordinator(schemaTableName);
        this.sleeperColumnHandlesInOrder = sleeperColumnHandlesInOrder;
    }

    /**
     * Appends a page of data to the Sleeper table. The rows will be stored locally, in memory or on disk, but may not
     * be written to S3 immediately.
     * <p>
     * The current implementation appears to operate synchronously, but it relies on the underlying {@link
     * IngestCoordinator} class which performs the actual upload to S3 in an asynchronous manner.
     *
     * @param  page The page of data to write.
     * @return      NOT_BLOCKED
     */
    @Override
    @SuppressFBWarnings("REC_CATCH_EXCEPTION") // Suppress for now, catch can probably be removed
    public CompletableFuture<?> appendPage(Page page) {
        try {
            noOfAppends++;
            noOfPositionsWritten += page.getPositionCount();
            ingestRecordsPageAsync.write(page);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return NOT_BLOCKED;
    }

    /**
     * Closes the ingest coordinator to ensure that all rows are written to S3. Propagates the CompletableFuture
     * returned from {@link IngestCoordinator}, which will complete once all of the data has been uploaded to S3.
     *
     * @return A {@link CompletableFuture} which completes once all the data has been written to S3. The future returns
     *         an empty list, which the Trino framework will eventually pass to {@link SleeperMetadata#finishInsert}.
     */
    @Override
    public CompletableFuture<Collection<Slice>> finish() {
        LOGGER.debug(String.format(
                "SleeperPageSink is finishing: %d positions written in %d appends, mean %.1f positions per append",
                noOfPositionsWritten, noOfAppends, ((float) noOfPositionsWritten) / noOfAppends));
        try {
            return ingestRecordsPageAsync.asyncCloseReturningResult().thenApply(dummy -> List.of());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Aborts any ingest future that may be running.
     */
    @Override
    public void abort() {
        ingestRecordsPageAsync.abort();
    }
}
