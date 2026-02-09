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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Ranges;
import io.trino.spi.predicate.TupleDomain;

import sleeper.trino.handle.SleeperColumnHandle;
import sleeper.trino.handle.SleeperSplit;
import sleeper.trino.handle.SleeperTableHandle;
import sleeper.trino.handle.SleeperTransactionHandle;
import sleeper.trino.remotesleeperconnection.SleeperConnectionAsTrino;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

/**
 * Provide a source of Sleeper table splits. Each split describes the scan of a single Sleeper partition and holds all
 * of the row key ranges within the partition that are to be scanned.
 * <p>
 * This class provides the ability to change the size of the batch of splits. Experience shows that this does not
 * usually make much difference to the way that Trino executes: it enthusiastically creates and schedules splits so that
 * each worker is as well-utilised as possible. The effect is that {@link #getNextBatch} is called more often when the
 * batches are small than it is when the batches are large, with the overarching outcome that the same number of splits
 * are read from this split source.
 */
public class SleeperSplitSource implements ConnectorSplitSource {
    private static final Logger LOGGER = Logger.get(SleeperSplitSource.class);

    private final SleeperConnectionAsTrino sleeperConnectionAsTrino;
    private final SleeperTransactionHandle sleeperTransactionHandle;
    private final SleeperTableHandle sleeperTableHandle;
    private final DynamicFilter dynamicFilter;
    private final int maxBatchSize;

    private Iterator<SleeperSplit> sleeperSplitIterator = null;

    public SleeperSplitSource(SleeperConnectionAsTrino sleeperConnectionAsTrino,
            SleeperTransactionHandle sleeperTransactionHandle,
            SleeperTableHandle sleeperTableHandle,
            DynamicFilter dynamicFilter,
            int maxBatchSize) {
        this.sleeperConnectionAsTrino = requireNonNull(sleeperConnectionAsTrino);
        this.sleeperTransactionHandle = requireNonNull(sleeperTransactionHandle);
        this.sleeperTableHandle = requireNonNull(sleeperTableHandle);
        this.dynamicFilter = requireNonNull(dynamicFilter);
        this.maxBatchSize = maxBatchSize;
    }

    /**
     * Create a list of splits. These are generated from a combination of the tuple domain returned by
     * {@link SleeperTableHandle#getTupleDomain()} and the additional tuple domain supplied as an argument.
     *
     * @param  sleeperConnectionAsTrino the connection to Sleeper to use to convert the ranges into splits
     * @param  sleeperTransactionHandle the transaction to do this operation under
     * @param  sleeperTableHandle       the table to generate the splits for
     * @param  additionalTupleDomain    an extra {@link TupleDomain} to intersect with the tupledomain from the
     *                                  {@link SleeperTableHandle}
     * @return                          a list of {@link SleeperSplit} objects
     */
    private static List<SleeperSplit> generateSleeperSplits(SleeperConnectionAsTrino sleeperConnectionAsTrino,
            SleeperTransactionHandle sleeperTransactionHandle,
            SleeperTableHandle sleeperTableHandle,
            TupleDomain<ColumnHandle> additionalTupleDomain) {
        // Combine the tuple domain from the table handle and the additional tuple domain
        TupleDomain<ColumnHandle> combinedTupleDomain = sleeperTableHandle.getTupleDomain().intersect(additionalTupleDomain);
        // Check that the combined tupledomain is legitimate and extract the ranges for the rowkey column
        Ranges ranges = verifyAndExtractRowKeyRanges(sleeperTableHandle, combinedTupleDomain);
        // Convert the ranges into a stream of splits and return it
        return sleeperConnectionAsTrino.generateSleeperSplits(
                sleeperTransactionHandle,
                sleeperTableHandle,
                ranges.getOrderedRanges());
    }

    /**
     * Create a future which will wait until the filter has narrowed as far as it possibly can.
     *
     * @param  dynamicFilter the dynamic filter that must be narrowed
     * @return               a {@link CompletableFuture} which completes when the dynamic filter has fully narrowed
     */
    private static CompletableFuture<?> futureWhenDynamicFilterHasNarrowedCompletely(DynamicFilter dynamicFilter) {
        // If the dynamic filter has fully narrowed, then return immediately
        if (!dynamicFilter.isAwaitable() || dynamicFilter.isComplete()) {
            return CompletableFuture.completedFuture(true);
        }
        // If it has not fully narrowed, then wait for it to narrow a bit and reuse this function to
        // allow it to narrow some more if it needs to.
        return dynamicFilter
                .isBlocked()
                .thenApply(dummy -> futureWhenDynamicFilterHasNarrowedCompletely(dynamicFilter));
    }

    /**
     * Perform verification checks on the table handle and tuple domain. Ensures that:
     * <ul>
     * <li>The table has just one rowkey column</li>
     * <li>The tupledomain applies a filter to the rowkey column</li>
     * <li>The tupledomain does not try to filter on any other columns</li>
     * </ul>
     * Exceptions are thrown if any of these conditions are not met.
     * <p>
     * Once the checks have passed, the {@link Ranges} corresponding to the rowkey column are extracted from the
     * tupledomain.
     *
     * @param  sleeperTableHandle       The Sleeper table handle
     * @param  tupleDomainToExtractFrom The tupledomain
     * @return                          The ranges which correspond to the single rowkey column
     */
    private static Ranges verifyAndExtractRowKeyRanges(SleeperTableHandle sleeperTableHandle,
            TupleDomain<ColumnHandle> tupleDomainToExtractFrom) {
        // Ensure that the table has a rowkey with a single field and extract the column handle of that field
        List<SleeperColumnHandle> rowKeySleeperColumnHandlesInOrder = sleeperTableHandle.getColumnHandlesInCategoryInOrder(SleeperColumnHandle.SleeperColumnCategory.ROWKEY);
        if (rowKeySleeperColumnHandlesInOrder.size() > 1) {
            throw new UnsupportedOperationException(
                    "This connector does not support Sleeper tables where the rowkeys have more than one column");
        }
        SleeperColumnHandle rowKeySleeperColumnHandle = rowKeySleeperColumnHandlesInOrder.get(0);
        // Ensure that a filter is applied to the rowkey column and extract it
        Optional<Map<ColumnHandle, Domain>> columnHandleDomainMapOpt = tupleDomainToExtractFrom.getDomains();
        if (columnHandleDomainMapOpt.isEmpty() || !columnHandleDomainMapOpt.get().containsKey(rowKeySleeperColumnHandle)) {
            throw new UnsupportedOperationException(
                    String.format("A filter must be applied to the rowkey column (%s) when querying a Sleeper table (%s.%s)",
                            rowKeySleeperColumnHandle.getColumnName(),
                            sleeperTableHandle.getSchemaTableName().getSchemaName(),
                            sleeperTableHandle.getSchemaTableName().getTableName()));
        }
        Map<ColumnHandle, Domain> columnHandleDomainMap = columnHandleDomainMapOpt.get();
        if (columnHandleDomainMap.size() > 1) {
            throw new AssertionError(String.format(
                    "Predicate pushdown is being applied to non-rowkey columns: %s",
                    columnHandleDomainMap.entrySet()));
        }
        // Retrieve the ranges corresponding to the domain of the row key
        return columnHandleDomainMap.get(rowKeySleeperColumnHandle).getValues().getRanges();
    }

    /**
     * Retrieve the next batch of splits.
     *
     * @param  maxBatchSize the maximum number of splits to include in the batch
     * @return              the batch of {@link SleeperSplit} objects, expressed as a batch of splits which will be
     *                      returned at some point in the future
     */
    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxBatchSize) {
        // Wait for a future where the dynamic filter has completely narrowed
        return futureWhenDynamicFilterHasNarrowedCompletely(this.dynamicFilter).thenApply(dummy -> {
            // Ensure that the sleeper split iterator has been initialised
            // The iterator is initialised once, then stored, and splits are removed from it as required
            // It is held as an iterator and not a stream because a stream must be processed in one go whereas an iterator
            // maintains its state as entries are retrieved from it
            if (sleeperSplitIterator == null) {
                sleeperSplitIterator = generateSleeperSplits(
                        sleeperConnectionAsTrino,
                        sleeperTransactionHandle,
                        sleeperTableHandle,
                        dynamicFilter.getCurrentPredicate()).iterator();
            }
            // Retrieve the next batch of splits from the iterator
            // This streaming construct is a convenient way to express this logic
            List<ConnectorSplit> splitsInNextBatch = StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(sleeperSplitIterator, Spliterator.NONNULL | Spliterator.IMMUTABLE),
                    false)
                    .limit(Math.min(this.maxBatchSize, maxBatchSize))
                    .collect(ImmutableList.toImmutableList());
            // Return the batch
            LOGGER.debug("SleeperSplitSource generating a batch of %d splits", splitsInNextBatch.size());
            return new ConnectorSplitBatch(splitsInNextBatch, isFinished());
        });
    }

    /**
     * Indicate whether this source can provide any more batches of splits.
     *
     * @return true if there are more batches available
     */
    @Override
    public boolean isFinished() {
        return sleeperSplitIterator != null && !sleeperSplitIterator.hasNext();
    }

    /**
     * Close the source of splits.
     */
    @Override
    public void close() {
        sleeperSplitIterator = null;
    }
}
