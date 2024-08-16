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
package sleeper.garbagecollector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableStatus;
import sleeper.core.util.LoggedDuration;
import sleeper.garbagecollector.FailedGarbageCollectionException.TableFailures;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static sleeper.configuration.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_ASYNC_COMMIT;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;

/**
 * Deletes files that are ready for garbage collection and removes them from the Sleeper table. Queries the
 * {@link StateStore} for files with no references, deletes the files, then updates the state store to remove them.
 */
public class GarbageCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(GarbageCollector.class);

    private final DeleteFile deleteFile;
    private final InstanceProperties instanceProperties;
    private final StateStoreProvider stateStoreProvider;

    public GarbageCollector(Configuration conf,
            InstanceProperties instanceProperties,
            StateStoreProvider stateStoreProvider) {
        this(filename -> deleteFileAndSketches(filename, conf),
                instanceProperties, stateStoreProvider);
    }

    public GarbageCollector(DeleteFile deleteFile,
            InstanceProperties instanceProperties,
            StateStoreProvider stateStoreProvider) {
        this.deleteFile = deleteFile;
        this.instanceProperties = instanceProperties;
        this.stateStoreProvider = stateStoreProvider;
    }

    public void run(List<TableProperties> tables) throws FailedGarbageCollectionException {
        runAtTime(Instant.now(), tables);
    }

    public void runAtTime(Instant startTime, List<TableProperties> tables) throws FailedGarbageCollectionException {
        LOGGER.info("Obtained list of {} tables", tables.size());
        int totalDeleted = 0;
        List<TableFailures> failedTables = new ArrayList<>();
        for (TableProperties tableProperties : tables) {
            TableStatus table = tableProperties.getStatus();
            TableFilesDeleted deleted = new TableFilesDeleted(table);
            try {
                LOGGER.info("Starting GC for table {}", table);
                deleteInBatches(tableProperties, startTime, deleted);
                LOGGER.info("{} files deleted for table {}", deleted.getDeletedFilenames().size(), table);
                totalDeleted += deleted.getDeletedFilenames().size();
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
    }

    private void deleteInBatches(TableProperties tableProperties, Instant startTime, TableFilesDeleted deleted) throws StateStoreException {
        int garbageCollectorBatchSize = instanceProperties.getInt(GARBAGE_COLLECTOR_BATCH_SIZE);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        Iterator<String> readyForGC = getReadyForGCIterator(tableProperties, startTime, stateStore);
        List<String> batch = new ArrayList<>();
        while (readyForGC.hasNext()) {
            String filename = readyForGC.next();
            batch.add(filename);
            if (batch.size() == garbageCollectorBatchSize) {
                deleteBatch(batch, tableProperties, stateStore, deleted);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            deleteBatch(batch, tableProperties, stateStore, deleted);
        }
    }

    private Iterator<String> getReadyForGCIterator(
            TableProperties tableProperties, Instant startTime, StateStore stateStore) throws StateStoreException {
        LOGGER.debug("Requesting iterator of files ready for garbage collection from state store");
        int delayBeforeDeletion = tableProperties.getInt(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION);
        Instant deletionTime = startTime.minus(delayBeforeDeletion, ChronoUnit.MINUTES);
        Iterator<String> readyForGC = stateStore.getReadyForGCFilenamesBefore(deletionTime).iterator();
        return readyForGC;
    }

    private void deleteBatch(List<String> batch, TableProperties tableProperties, StateStore stateStore, TableFilesDeleted deleted) {
        List<String> deletedFilenames = deleteFiles(batch, deleted);
        LOGGER.info("Deleted {} files in batch", deletedFilenames.size());
        try {
            boolean asyncCommit = tableProperties.getBoolean(GARBAGE_COLLECTOR_ASYNC_COMMIT);
            if (asyncCommit) {

            } else {
                stateStore.deleteGarbageCollectedFileReferenceCounts(deletedFilenames);
                LOGGER.info("Applied deletion to state store");
            }
        } catch (Exception e) {
            LOGGER.error("Failed to update state store for files: {}", deletedFilenames, e);
            deleted.failedStateStoreUpdate(deletedFilenames, e);
        }
    }

    private List<String> deleteFiles(List<String> filenames, TableFilesDeleted deleted) {
        List<String> deletedFilenames = new ArrayList<>(filenames.size());
        for (String filename : filenames) {
            try {
                deleteFile.deleteFileAndSketches(filename);
                deleted.deleted(filename);
                deletedFilenames.add(filename);
            } catch (Exception e) {
                LOGGER.error("Failed to delete file: {}", filename, e);
                deleted.failed(filename, e);
            }
        }
        return deletedFilenames;
    }

    @FunctionalInterface
    public interface DeleteFile {
        void deleteFileAndSketches(String filename) throws IOException;
    }

    private static void deleteFileAndSketches(String filename, Configuration conf) throws IOException {
        deleteFile(filename, conf);
        String sketchesFile = filename.replace(".parquet", ".sketches");
        deleteFile(sketchesFile, conf);
    }

    private static void deleteFile(String filename, Configuration conf) throws IOException {
        Path path = new Path(filename);
        FileSystem fileSystem = path.getFileSystem(conf);
        if (!fileSystem.exists(path)) {
            LOGGER.warn("File did not exist: {}", filename);
            return;
        }
        boolean success = path.getFileSystem(conf).delete(path, false);
        if (!success) {
            throw new IOException("File could not be deleted: " + filename);
        }
        LOGGER.info("Deleted file {}", filename);
    }
}
