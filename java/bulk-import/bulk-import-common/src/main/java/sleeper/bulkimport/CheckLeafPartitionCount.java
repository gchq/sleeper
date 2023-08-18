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

package sleeper.bulkimport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;

import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;

public class CheckLeafPartitionCount {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckLeafPartitionCount.class);

    private CheckLeafPartitionCount() {
    }

    public static boolean hasMinimumPartitions(
            StateStoreProvider stateStoreProvider, TablePropertiesProvider tablePropertiesProvider, BulkImportJob job) {
        TableProperties tableProperties = tablePropertiesProvider.getTableProperties(job.getTableName());

        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        int leafPartitionCount;
        try {
            leafPartitionCount = stateStore.getLeafPartitions().size();
        } catch (StateStoreException e) {
            throw new RuntimeException("Failed to get leaf partition count", e);
        }
        int minPartitionCount = tableProperties.getInt(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT);
        if (leafPartitionCount < minPartitionCount) {
            LOGGER.info("Minimum partition count was {}, but found {} leaf partitions. Skipping job {}",
                    minPartitionCount, leafPartitionCount, job.getId());
            return false;
        } else {
            LOGGER.info("Minimum partition count has been met. Running job {}", job.getId());
            return true;
        }
    }
}
