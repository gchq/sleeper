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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.transaction.IsolationLevel;

import sleeper.trino.procedure.SleeperProcedureHello;
import sleeper.trino.remotesleeperconnection.SleeperConnectionAsTrino;
import sleeper.trino.systemtable.SleeperSystemTablePartitions;
import sleeper.trino.systemtable.SleeperSystemTableRandom;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * A Trino connector to a Sleeper instance. Provides the Trino framework with access to many of the core management and
 * provider classes.
 */
public class SleeperConnector implements Connector {
    private final SleeperConnectionAsTrino sleeperConnectionAsTrino;
    private final SleeperMetadata sleeperMetadata;
    private final SleeperSplitManager sleeperSplitManager;
    private final SleeperRecordSetProvider sleeperRecordSetProvider;
    private final SleeperPageSinkProvider sleeperPageSinkProvider;

    @Inject
    public SleeperConnector(SleeperConnectionAsTrino sleeperConnectionAsTrino,
            SleeperMetadata sleeperMetadata,
            SleeperSplitManager sleeperSplitManager,
            SleeperRecordSetProvider sleeperRecordSetProvider,
            SleeperPageSinkProvider sleeperPageSinkProvider) {
        this.sleeperConnectionAsTrino = requireNonNull(sleeperConnectionAsTrino);
        this.sleeperMetadata = requireNonNull(sleeperMetadata);
        this.sleeperSplitManager = requireNonNull(sleeperSplitManager);
        this.sleeperRecordSetProvider = requireNonNull(sleeperRecordSetProvider);
        this.sleeperPageSinkProvider = requireNonNull(sleeperPageSinkProvider);
    }

    /**
     * Tracks when a transaction begins. Called whenever a new transaction is started via START TRANSACTION or when a
     * single query is executed outside of a surrounding transaction.
     *
     * @param  isolationLevel The isolation level of the transaction. All Sleeper queries via Trino are read-only and so
     *                        we assume that they are all SERIALIZABLE. This may not be true if data is being ingested
     *                        at the same time.
     * @param  readOnly       Single queries always have read-only set to false, even if they are SELECT queries, and
     *                        so we cannot use this parameter to reject a transaction is attempting to write data.
     * @param  autoCommit     Whether the transaction uses auto-commit mode.
     * @return                the transaction handle
     */
    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return sleeperConnectionAsTrino.getNewSleeperTransactionHandle();
    }

    /**
     * Provide details about the schema and table structures, procedures and so on to the Trino Framework. The
     * {@link SleeperMetadata} class that is returned also handles the application of a pushed-down filter to the
     * Sleeper table.
     *
     * @param  session           The current connector session.
     * @param  transactionHandle The current transaction. The metadata is assumed to be constant across transactions.
     * @return                   the {@link SleeperMetadata} object
     */
    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return sleeperMetadata;
    }

    /**
     * Provide details about the splits to the Trino framework. Splits are used to partition parts of a table-scan
     * across different workers.
     *
     * @return the {@link SleeperSplitManager} object, which is used to generate splits
     */
    @Override
    public ConnectorSplitManager getSplitManager() {
        return sleeperSplitManager;
    }

    /**
     * Returns a provider to create record sets. These can in turn be used to read data from Sleeper.
     */
    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return sleeperRecordSetProvider;
    }

    /**
     * Returns a page sink provider to support INSERT operations.
     *
     * @return the {@link SleeperPageSinkProvider} to use to provide the sinks
     */
    @Override
    public ConnectorPageSinkProvider getPageSinkProvider() {
        return sleeperPageSinkProvider;
    }

    /**
     * Provide a set of system tables to the Trino framework. These system tables are used here to provide access to,
     * for example, a table of all of the partitions which sit under any Sleeper table.
     *
     * @return A set of system tables.
     */
    @Override
    public Set<SystemTable> getSystemTables() {
        return ImmutableSet.of(
                new SleeperSystemTableRandom(),
                new SleeperSystemTablePartitions(sleeperConnectionAsTrino));
    }

    /**
     * Provide a set of procedures to the Trino framework. Procedures are called using the SQL CALL command. They ma
     * take arguments but they cannot return results.
     *
     * @return A set of procedures.
     */
    @Override
    public Set<Procedure> getProcedures() {
        return ImmutableSet.of(SleeperProcedureHello.getProcedure());
    }

    /**
     * Provide a set of connector capabilities to indicate to the Trino framework what this connector will support. At
     * present, the only capability which can be indicated is {@link ConnectorCapabilities#NOT_NULL_COLUMN_CONSTRAINT}
     * and this does not appear to make any difference to the Trino framework, which seems to ignore the 'nullable'
     * field returned by the {@link SleeperMetadata} class.
     *
     * @return The set of connector capabilities.
     */
    @Override
    public Set<ConnectorCapabilities> getCapabilities() {
        // Setting this value does not seem to make any difference to whether not-null flags are returned by Trino
        return ImmutableSet.of(ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT);
    }

    /**
     * Return a provider to map to Sleeper partitions. This allows rows of data to be directed to different partitions
     * as required during a write operation, and splits to be assigned to specific nodes during a read operation.
     *
     * @return the node-partitioning provider
     */
    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider() {
        return new SleeperNodePartitioningProvider();
    }

    /**
     * Shut down the connector, freeing all resources.
     */
    @Override
    public void shutdown() {
        sleeperConnectionAsTrino.close();
    }
}
