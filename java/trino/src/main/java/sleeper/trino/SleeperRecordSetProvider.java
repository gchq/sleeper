/*
 * Copyright 2022-2023 Crown Copyright
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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import sleeper.trino.handle.SleeperColumnHandle;
import sleeper.trino.handle.SleeperSplit;
import sleeper.trino.handle.SleeperTransactionHandle;
import sleeper.trino.remotesleeperconnection.SleeperConnectionAsTrino;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Provides a {@link SleeperRecordSet} which will scan an entire {@link SleeperSplit} and return the records in that
 * split.
 */
public class SleeperRecordSetProvider implements ConnectorRecordSetProvider {
    private final SleeperConnectionAsTrino sleeperConnectionAsTrino;

    @Inject
    public SleeperRecordSetProvider(SleeperConnectionAsTrino sleeperConnectionAsTrino) {
        this.sleeperConnectionAsTrino = requireNonNull(sleeperConnectionAsTrino);
    }

    /**
     * Provide a record set according to the supplied parameters.
     *
     * @param transactionHandle          The transaction that the record set is to run under.
     * @param session                    The session that the record set is to run under.
     * @param split                      The split that the record set is to read. The split contains tne details of the
     *                                   Sleeper partition, and the rowkey ranges within that partition, that are to be
     *                                   read.
     * @param tableHandle                The table that the record set is to read. Note that the tupledomain returned by
     *                                   {@link SleeperTableHandle#getTupleDomain()} is ignored and the ranges retrieved
     *                                   from the split are used instead.
     * @param outputColumnHandlesInOrder The column handles to be returned by the record set.
     * @return The record set which corresponds to the supplied parameters.
     */
    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle,
                                  ConnectorSession session,
                                  ConnectorSplit split,
                                  ConnectorTableHandle tableHandle,
                                  List<? extends ColumnHandle> outputColumnHandlesInOrder) {
        List<SleeperColumnHandle> sleeperColumnHandles = outputColumnHandlesInOrder.stream()
                .map(SleeperColumnHandle.class::cast)
                .collect(ImmutableList.toImmutableList());

        return new SleeperRecordSet(
                sleeperConnectionAsTrino,
                (SleeperTransactionHandle) transactionHandle,
                (SleeperSplit) split,
                sleeperColumnHandles);
    }
}
