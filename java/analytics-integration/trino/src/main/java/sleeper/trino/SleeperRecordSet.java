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
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import sleeper.trino.handle.SleeperColumnHandle;
import sleeper.trino.handle.SleeperSplit;
import sleeper.trino.handle.SleeperTableHandle;
import sleeper.trino.handle.SleeperTransactionHandle;
import sleeper.trino.remotesleeperconnection.SleeperConnectionAsTrino;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Generates a cursor to scan through rows of data retrieved from Sleeper. Each instance of this class is constructed
 * from a {@link SleeperSplit}. The split contains the details of the Sleeper partition, files and all of the key ranges
 * within that partition to be scanned. The key ranges may be very narrow and so this record set can perform highly
 * selective scans as well as broader scans.
 */
public class SleeperRecordSet implements RecordSet {
    private static final Logger LOGGER = Logger.get(SleeperRecordSet.class);

    private final SleeperSplit sleeperSplit;
    private final List<SleeperColumnHandle> outputSleeperColumnHandlesInOrder;
    private final Stream<List<Object>> resultRowStream;

    /**
     * Construct a record set which scans an entire split. The {@link SleeperSplit} contains partition and range
     * information, so this scan may be small or large, depending on the parameters in the split.
     * <p>
     * Note that the {@link io.trino.spi.predicate.TupleDomain} returned by {@link SleeperTableHandle#getTupleDomain()}
     * is ignored and the rowkey ranges from within the split are used instead.
     *
     * @param sleeperConnectionAsTrino          The Sleeper connection
     * @param sleeperTransactionHandle          The transaction that this record set is to be generated under
     * @param sleeperSplit                      The split that is to be scanned
     * @param outputSleeperColumnHandlesInOrder The columns that are to be returned
     */
    public SleeperRecordSet(SleeperConnectionAsTrino sleeperConnectionAsTrino,
            SleeperTransactionHandle sleeperTransactionHandle,
            SleeperSplit sleeperSplit,
            List<SleeperColumnHandle> outputSleeperColumnHandlesInOrder) {
        requireNonNull(sleeperConnectionAsTrino);
        requireNonNull(sleeperTransactionHandle);
        requireNonNull(sleeperSplit);
        this.sleeperSplit = sleeperSplit;
        this.outputSleeperColumnHandlesInOrder = requireNonNull(outputSleeperColumnHandlesInOrder);
        this.resultRowStream = sleeperConnectionAsTrino.streamEntireSplitResultRows(
                sleeperTransactionHandle,
                sleeperSplit,
                outputSleeperColumnHandlesInOrder);
    }

    /**
     * Retrieve the Trino types of all the columns returned by the cursor.
     *
     * @return the types of the columns, in the order they will be returned
     */
    @Override
    public List<Type> getColumnTypes() {
        return outputSleeperColumnHandlesInOrder.stream()
                .map(SleeperColumnHandle::getColumnTrinoType)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Retrieve a cursor which allows Trino to step through the resulting rows.
     *
     * @return the {@link SleeperRecordCursor}
     */
    @Override
    public RecordCursor cursor() {
        LOGGER.debug("Record set is creating a record cursor for query %s", sleeperSplit.getLeafPartitionQuery().getQueryId());
        return new SleeperRecordCursor(sleeperSplit.getLeafPartitionQuery().getQueryId(), getColumnTypes(), resultRowStream);
    }
}
