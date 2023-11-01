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
package sleeper.garbagecollector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableIdentity;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Queries the {@link StateStore} for files that are marked as being ready for
 * garbage collection, and deletes them.
 */
public class GarbageCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(GarbageCollector.class);

    private final Configuration conf;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final int garbageCollectorBatchSize;

    public GarbageCollector(Configuration conf,
                            TablePropertiesProvider tablePropertiesProvider,
                            StateStoreProvider stateStoreProvider,
                            int garbageCollectorBatchSize) {
        this.conf = conf;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.garbageCollectorBatchSize = garbageCollectorBatchSize;
    }

    public void run() throws StateStoreException, IOException {
        long startTimeEpochSecs = LocalDateTime.now().atZone(ZoneId.systemDefault()).toEpochSecond();
        int totalDeleted = 0;
        List<TableProperties> tables = tablePropertiesProvider.streamAllTables()
                .collect(Collectors.toUnmodifiableList());
        LOGGER.info("Obtained list of {} tables", tables.size());

        for (TableProperties tableProperties : tables) {
            TableIdentity tableId = tableProperties.getId();
            LOGGER.info("Obtaining StateStore for table {}", tableId);
            StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);

            LOGGER.debug("Requesting iterator of files ready for garbage collection from state store");
            Iterator<FileInfo> readyForGC = stateStore.getReadyForGCFiles();

            int numberDeleted = 0;
            while (readyForGC.hasNext() && numberDeleted < garbageCollectorBatchSize) {
                FileInfo fileInfo = readyForGC.next();
                deleteFileAndUpdateStateStore(fileInfo, stateStore, conf);
                numberDeleted++;
            }
            LOGGER.info("{} files deleted for table {}", numberDeleted, tableId);
            totalDeleted += numberDeleted;
        }
        long endTimeEpochSecs = LocalDateTime.now()
                .atZone(ZoneId.systemDefault())
                .toEpochSecond();
        int runTime = (int) (endTimeEpochSecs - startTimeEpochSecs);
        LOGGER.info("{} files deleted in {} seconds", totalDeleted, runTime);
    }

    private void deleteFileAndUpdateStateStore(FileInfo fileInfo, StateStore stateStore, Configuration conf) throws IOException {
        deleteFiles(fileInfo.getFilename(), conf);
        try {
            stateStore.deleteReadyForGCFile(fileInfo);
        } catch (StateStoreException e) {
            LOGGER.error("Exception updating status of " + fileInfo.getFilename() + " to garbage collected", e);
        }
    }

    private void deleteFiles(String filename, Configuration conf) throws IOException {
        deleteFile(filename, conf);
        String sketchesFile = filename.replace(".parquet", ".sketches");
        deleteFile(sketchesFile, conf);
    }

    private void deleteFile(String filename, Configuration conf) throws IOException {
        Path path = new Path(filename);
        path.getFileSystem(conf).delete(path, false);
        LOGGER.info("Deleted file {}", filename);
    }
}
