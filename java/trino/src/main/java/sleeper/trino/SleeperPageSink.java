package sleeper.trino;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.SchemaTableName;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.trino.handle.SleeperColumnHandle;
import sleeper.trino.remotesleeperconnection.SleeperConnectionAsTrino;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * This class provides a way to write rows into Sleeper, such as when an INSERT statement is run.
 * <p>
 * This class currently references the Sleeper-internal {@link IngestCoordinator} class. It may be a good idea to reduce
 * the spread of internal classes like this so that the Sleeper-specific code all lives within {@link
 * SleeperConnectionAsTrino}.
 * <p>
 * The error handling is limited within this class.
 */
public class SleeperPageSink implements ConnectorPageSink {
    private static final Logger LOG = Logger.get(SleeperPageSink.class);

    private final IngestCoordinator<Page> ingestRecordsPageAsync;
    private final List<SleeperColumnHandle> sleeperColumnHandlesInOrder;
    private long noOfAppends = 0L;
    private long noOfPositionsWritten = 0L;

    /**
     * Construct the page sink.
     *
     * @param sleeperConnectionAsTrino    The connection to Sleeper.
     * @param schemaTableName             The table to append rows to.
     * @param sleeperColumnHandlesInOrder The columns which are to be appended. These must currently be all of the
     *                                    columns in the table.
     */
    public SleeperPageSink(SleeperConnectionAsTrino sleeperConnectionAsTrino,
                           SchemaTableName schemaTableName,
                           List<SleeperColumnHandle> sleeperColumnHandlesInOrder) {
        LOG.debug("Creating a SleeperPageSink for %s", schemaTableName.getTableName());
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
     * @param page The page of data to write.
     * @return NOT_BLOCKED
     */
    @Override
    public CompletableFuture<?> appendPage(Page page) {
        try {
            this.noOfAppends++;
            this.noOfPositionsWritten += page.getPositionCount();
            this.ingestRecordsPageAsync.write(page);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return NOT_BLOCKED;
    }

    /**
     * Closes the {@link IngestCoordinator} class to ensure that all rows are written to S3.
     * <p>
     * The current implementation uses the future that is return from {@link IngestCoordinator}, which will complete
     * once all of the data has been uploaded to S3.
     *
     * @return A {@link CompletableFuture} which completes once all the data has been written to S3. The future returns
     * an empty list, which the Trino framework will eventually pass to {@link SleeperMetadata#finishInsert}.
     */
    @Override
    public CompletableFuture<Collection<Slice>> finish() {
        LOG.debug(String.format(
                "SleeperPageSink is finishing: %d positions written in %d appends, mean %.1f positions per append",
                this.noOfPositionsWritten, this.noOfAppends, ((float) this.noOfPositionsWritten) / this.noOfAppends));
        try {
            return this.ingestRecordsPageAsync.asyncCloseReturningFileInfoList().thenApply(dummy -> ImmutableList.of());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Aborts any ingest future that may be running.
     */
    @Override
    public void abort() {
        this.ingestRecordsPageAsync.abort();
    }
}
