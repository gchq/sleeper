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
package sleeper.statestore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Schema;

import java.util.List;

/**
 * Initialises a {@link StateStore} from the given list of {@link Partition}s. Utility methods
 * are provided to allow an instance of this class to be created from a list of split points.
 */
public class InitialiseStateStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(InitialiseStateStore.class);

    private final StateStore stateStore;
    private final List<Partition> initialPartitions;

    public InitialiseStateStore(StateStore stateStore,
                                List<Partition> initialPartitions) {
        this.stateStore = stateStore;
        this.initialPartitions = initialPartitions;
    }

    public static InitialiseStateStore createInitialiseStateStoreFromSplitPoints(TableProperties tableProperties,
            StateStore stateStore, List<Object> splitPoints) {
        return createInitialiseStateStoreFromSplitPoints(tableProperties.getSchema(), stateStore, splitPoints);
    }

    public static InitialiseStateStore createInitialiseStateStoreFromSplitPoints(Schema schema, StateStore stateStore,
            List<Object> splitPoints) {
        PartitionsFromSplitPoints partitionsFromSplitPoints = new PartitionsFromSplitPoints(schema, splitPoints);
        List<Partition> initialPartitions = partitionsFromSplitPoints.construct();

        return new InitialiseStateStore(stateStore, initialPartitions);
    }

    public void run() throws StateStoreException {
        // Validate that this appears to be an empty table
        List<Partition> partitions = stateStore.getAllPartitions();
        if (!partitions.isEmpty()) {
            LOGGER.error("This should only be run on a database on which no data has been ingested - this instance has " + partitions.size() + " partitions");
            return;
        }
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        if (!activeFiles.isEmpty()) {
            LOGGER.error("This should only be run on a database on which no data has been ingested - this instance has " + activeFiles.size() + " active files");
            return;
        }
        LOGGER.info("Database appears to be empty");

        stateStore.initialise(initialPartitions);
    }
}
