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
package sleeper.garbagecollector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.statestore.transactionlog.transaction.impl.DeleteFilesTransaction;
import sleeper.core.table.TableStatus;
import sleeper.core.util.LoggedDuration;
import sleeper.garbagecollector.FailedGarbageCollectionException.TableFailures;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static sleeper.core.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_BATCH_SIZE;
import static sleeper.core.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_MAXIMUM_FILE_DELETION_PER_INVOCATION;
import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_ASYNC_COMMIT;
import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * Deletes files that are ready for garbage collection and removes them from the Sleeper table. Queries the
 * {@link StateStore} for files with no references, deletes the files, then updates the state store to remove them.
 */
public class GarbageCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(GarbageCollector.class);

    private final DeleteFiles deleteFiles;
    private final InstanceProperties instanceProperties;
    private final StateStoreProvider stateStoreProvider;
    private final StateStoreCommitRequestSender sendAsyncCommit;

    public GarbageCollector(DeleteFiles deleteFiles,
            InstanceProperties instanceProperties,
            StateStoreProvider stateStoreProvider,
            StateStoreCommitRequestSender sendAsyncCommit) {
        this.deleteFiles = deleteFiles;
        this.instanceProperties = instanceProperties;
        this.stateStoreProvider = stateStoreProvider;
        this.sendAsyncCommit = sendAsyncCommit;
    }

    public int run(List<TableProperties> tables) throws FailedGarbageCollectionException {
        return runAtTime(Instant.now(), tables);
    }

    public int runAtTime(Instant startTime, List<TableProperties> tables) throws FailedGarbageCollectionException {
        LOGGER.info("Obtained list of {} tables", tables.size());
        int totalDeleted = 0;
        List<TableFailures> failedTables = new ArrayList<>();
        for (TableProperties tableProperties : tables) {
            TableStatus table = tableProperties.getStatus();
            TableFilesDeleted deleted = new TableFilesDeleted(table);
            try {
                LOGGER.info("Starting GC for table {}", table);
                deleteInBatches(tableProperties, startTime, deleted);
                LOGGER.info("{} files deleted for table {}", deleted.getDeletedFileCount(), table);
                totalDeleted += deleted.getDeletedFileCount();
                deleted.buildTableFailures().ifPresent(failedTables::add);
            } catch (Exception e) {
                LOGGER.info("Failed to collect garbage for table {}", table, e);
                failedTables.add(deleted.buildTableFailures(e));
            }
        }
        LoggedDuration duration = LoggedDuration.withFullOutput(startTime, Instant.now());
        LOGGER.info("{} files deleted in {}", totalDeleted, duration);
        if (!failedTables.isEmpty()) {
            throw new FailedGarbageCollectionException(failedTables);
        }
        return totalDeleted;
    }

    private void deleteInBatches(TableProperties tableProperties, Instant startTime, TableFilesDeleted deleted) {
        int batchSize = instanceProperties.getInt(GARBAGE_COLLECTOR_BATCH_SIZE);
        int maxFiles = instanceProperties.getInt(GARBAGE_COLLECTOR_MAXIMUM_FILE_DELETION_PER_INVOCATION);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        Iterator<String> readyForGC = getReadyForGCIterator(tableProperties, startTime, stateStore);
        List<String> batch = new ArrayList<>();
        while (readyForGC.hasNext() &&
                deleted.getDeletedFileCount() < maxFiles) {
            String filename = readyForGC.next();
            batch.add(filename);
            if (batch.size() == batchSize) {
                deleteBatch(batch, tableProperties, stateStore, deleted);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            deleteBatch(batch, tableProperties, stateStore, deleted);
        }
    }

    private Iterator<String> getReadyForGCIterator(
            TableProperties tableProperties, Instant startTime, StateStore stateStore) {
        LOGGER.debug("Requesting iterator of files ready for garbage collection from state store");
        int delayBeforeDeletion = tableProperties.getInt(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION);
        Instant deletionTime = startTime.minus(delayBeforeDeletion, ChronoUnit.MINUTES);
        Iterator<String> readyForGC = stateStore.getReadyForGCFilenamesBefore(deletionTime).iterator();
        return readyForGC;
    }

    private void deleteBatch(List<String> batch, TableProperties tableProperties, StateStore stateStore, TableFilesDeleted deleted) {
        deleted.startBatch(batch);
        deleteFiles.deleteFiles(batch, deleted);
        List<String> deletedFilenames = deleted.getFilesDeletedInBatch();
        LOGGER.info("Deleted {} files in batch", deletedFilenames.size());
        try {
            boolean asyncCommit = tableProperties.getBoolean(GARBAGE_COLLECTOR_ASYNC_COMMIT);
            if (asyncCommit) {
                sendAsyncCommit.send(StateStoreCommitRequest.create(
                        tableProperties.get(TABLE_ID), new DeleteFilesTransaction(deletedFilenames)));
                LOGGER.info("Submitted asynchronous request to state store committer for {} deleted files in table {}", deletedFilenames.size(), tableProperties.getStatus());
            } else {
                new DeleteFilesTransaction(deletedFilenames).synchronousCommit(stateStore);
                LOGGER.info("Applied deletion to state store");
            }
        } catch (Exception e) {
            LOGGER.error("Failed to update state store for files: {}", deletedFilenames, e);
            deleted.failedStateStoreUpdate(deletedFilenames, e);
        }
    }

    @FunctionalInterface
    public interface DeleteFiles {
        void deleteFiles(List<String> filenames, TableFilesDeleted deleted);
    }
}
