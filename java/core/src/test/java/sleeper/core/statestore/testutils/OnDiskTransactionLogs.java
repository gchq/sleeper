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
package sleeper.core.statestore.testutils;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.LoadLocalProperties;
import sleeper.core.properties.local.SaveLocalProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.log.DuplicateTransactionNumberException;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.log.TransactionLogRange;
import sleeper.core.statestore.transactionlog.log.TransactionLogStore;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * Caches transactions on disk.
 */
public class OnDiskTransactionLogs {

    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final TransactionLogStore filesLogStore;
    private final TransactionLogStore partitionsLogStore;

    private OnDiskTransactionLogs(InstanceProperties instanceProperties, TableProperties tableProperties, TransactionLogStore filesLogStore, TransactionLogStore partitionsLogStore) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.filesLogStore = filesLogStore;
        this.partitionsLogStore = partitionsLogStore;
    }

    /**
     * Creates the path to the local temporary directory where the table state can be cached.
     *
     * @param  instanceId the Sleeper instance ID
     * @param  tableId    the Sleeper table ID
     * @return            the directory
     */
    public static Path getLocalCacheDirectory(String instanceId, String tableId) {
        return Paths.get("/tmp/sleeper/stateCache").resolve(instanceId).resolve(tableId);
    }

    /**
     * Writes the state of a table from its transaction logs to a local directory.
     *
     * @param  instanceProperties   the instance properties
     * @param  tableProperties      the table properties
     * @param  filesLogStore        the files transaction log
     * @param  partitionsLogStore   the partitions transaction log
     * @param  transactionBodyStore the transaction body store
     * @param  cacheDirectory       the local directory
     * @return                      the local cache
     */
    public static OnDiskTransactionLogs cacheState(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            TransactionLogStore filesLogStore, TransactionLogStore partitionsLogStore,
            TransactionBodyStore transactionBodyStore, Path cacheDirectory) {
        Path filesLogDir = cacheDirectory.resolve("filesLog");
        Path partitionsLogDir = cacheDirectory.resolve("partitionsLog");
        Path configDir = cacheDirectory.resolve("config");
        try {
            Files.createDirectories(filesLogDir);
            Files.createDirectories(partitionsLogDir);
            Files.createDirectories(configDir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        TransactionSerDe transactionSerDe = new TransactionSerDe(tableProperties.getSchema());
        TransactionLogStore filesCache = OnDiskTransactionLogStore.inDirectory(filesLogDir, transactionSerDe);
        TransactionLogStore partitionsCache = OnDiskTransactionLogStore.inDirectory(partitionsLogDir, transactionSerDe);
        copyTransactionsWithBodies(tableProperties.get(TABLE_ID), filesLogStore, filesCache, transactionBodyStore);
        copyTransactionsWithBodies(tableProperties.get(TABLE_ID), partitionsLogStore, partitionsCache, transactionBodyStore);
        try {
            SaveLocalProperties.saveToDirectory(configDir, instanceProperties, Stream.of(tableProperties));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new OnDiskTransactionLogs(instanceProperties, tableProperties, filesCache, partitionsCache);
    }

    /**
     * Loads the state of a table from a local directory.
     *
     * @param  cacheDirectory the directory
     * @return                the local cache
     */
    public static OnDiskTransactionLogs load(Path cacheDirectory) {
        Path configDir = cacheDirectory.resolve("config");
        InstanceProperties instanceProperties = LoadLocalProperties.loadInstancePropertiesFromDirectory(configDir);
        TableProperties tableProperties = LoadLocalProperties.loadTablesFromDirectory(instanceProperties, configDir)
                .findFirst().orElseThrow();
        TransactionSerDe transactionSerDe = new TransactionSerDe(tableProperties.getSchema());
        TransactionLogStore filesCache = OnDiskTransactionLogStore.inDirectory(cacheDirectory.resolve("filesLog"), transactionSerDe);
        TransactionLogStore partitionsCache = OnDiskTransactionLogStore.inDirectory(cacheDirectory.resolve("partitionsLog"), transactionSerDe);
        return new OnDiskTransactionLogs(instanceProperties, tableProperties, filesCache, partitionsCache);
    }

    public InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public TableProperties getTableProperties() {
        return tableProperties;
    }

    public TransactionLogStore getFilesLogStore() {
        return filesLogStore;
    }

    public TransactionLogStore getPartitionsLogStore() {
        return partitionsLogStore;
    }

    private static void copyTransactionsWithBodies(String tableId, TransactionLogStore source, TransactionLogStore target, TransactionBodyStore bodyStore) {
        readAllTransactionsWithBodies(tableId, source, bodyStore)
                .forEach(entry -> {
                    try {
                        target.addTransaction(entry);
                    } catch (DuplicateTransactionNumberException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private static Stream<TransactionLogEntry> readAllTransactionsWithBodies(String tableId, TransactionLogStore logStore, TransactionBodyStore bodyStore) {
        return logStore.readTransactions(TransactionLogRange.fromMinimum(1))
                .map(entry -> {
                    StateStoreTransaction<?> transaction = entry.getTransactionOrLoadFromPointer(tableId, bodyStore);
                    return new TransactionLogEntry(entry.getTransactionNumber(), entry.getUpdateTime(), transaction);
                });
    }

}
