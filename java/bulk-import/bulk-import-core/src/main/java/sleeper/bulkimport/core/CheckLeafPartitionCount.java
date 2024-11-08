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

package sleeper.bulkimport.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;

public class CheckLeafPartitionCount {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckLeafPartitionCount.class);

    private CheckLeafPartitionCount() {
    }

    public static boolean hasMinimumPartitions(
            TableProperties tableProperties, StateStore stateStore, BulkImportJob job) {
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
