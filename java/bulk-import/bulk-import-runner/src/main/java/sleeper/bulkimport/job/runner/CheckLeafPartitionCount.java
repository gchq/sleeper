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

package sleeper.bulkimport.job.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;

import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_MIN_PARTITION_COUNT;

public class CheckLeafPartitionCount {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckLeafPartitionCount.class);

    private CheckLeafPartitionCount() {
    }

    public static boolean hasMinimumPartitions(
            StateStoreProvider stateStoreProvider, TablePropertiesProvider tablePropertiesProvider, String tableName)
            throws StateStoreException {
        TableProperties tableProperties = tablePropertiesProvider.getTableProperties(tableName);
        return hasMinimumPartitions(stateStoreProvider.getStateStore(tableProperties), tableProperties);
    }

    public static boolean hasMinimumPartitions(StateStore stateStore, TableProperties tableProperties)
            throws StateStoreException {
        int leafPartitionCount = stateStore.getLeafPartitions().size();
        int minPartitionCount = tableProperties.getInt(BULK_IMPORT_MIN_PARTITION_COUNT);
        if (leafPartitionCount < minPartitionCount) {
            LOGGER.info("Minimum partition count was {}, but found {} leaf partitions.",
                    minPartitionCount, leafPartitionCount);
            return false;
        } else {
            return true;
        }
    }
}
