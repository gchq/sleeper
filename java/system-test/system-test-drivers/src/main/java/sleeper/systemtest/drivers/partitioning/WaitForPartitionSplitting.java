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

package sleeper.systemtest.drivers.partitioning;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.PollWithRetries;
import sleeper.splitter.FindPartitionToSplitResult;
import sleeper.splitter.FindPartitionsToSplit;
import sleeper.statestore.StateStoreProvider;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class WaitForPartitionSplitting {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForPartitionSplitting.class);

    private static final long POLL_INTERVAL_MILLIS = 5000;
    private static final int MAX_POLLS = 12;
    private final Map<String, Set<String>> partitionIdsByTableId;

    private WaitForPartitionSplitting(List<FindPartitionToSplitResult> toSplit) {
        partitionIdsByTableId = toSplit.stream()
                .collect(Collectors.groupingBy(FindPartitionToSplitResult::getTableId,
                        Collectors.mapping(split -> split.getPartition().getId(), Collectors.toSet())));
    }

    public static WaitForPartitionSplitting forCurrentPartitionsNeedingSplitting(
            TablePropertiesProvider propertiesProvider, StateStoreProvider stateStoreProvider) {
        return new WaitForPartitionSplitting(
                FindPartitionsToSplit.getResults(propertiesProvider, stateStoreProvider));
    }

    public void pollUntilFinished(TablePropertiesProvider propertiesProvider, StateStoreProvider stateStoreProvider) throws InterruptedException {
        LOGGER.info("Waiting for splits, expecting partitions to be split: {}", partitionIdsByTableId.keySet());
        PollWithRetries.intervalAndMaxPolls(POLL_INTERVAL_MILLIS, MAX_POLLS)
                .pollUntil("partition splits finished", () ->
                        new FinishedCheck(propertiesProvider, stateStoreProvider).isFinished());
    }

    public boolean isSplitFinished(TablePropertiesProvider propertiesProvider, StateStoreProvider stateStoreProvider) {
        return new FinishedCheck(propertiesProvider, stateStoreProvider).isFinished();
    }

    private class FinishedCheck {
        private final TablePropertiesProvider propertiesProvider;
        private final StateStoreProvider stateStoreProvider;

        FinishedCheck(TablePropertiesProvider propertiesProvider, StateStoreProvider stateStoreProvider) {
            this.propertiesProvider = propertiesProvider;
            this.stateStoreProvider = stateStoreProvider;
        }

        public boolean isFinished() {
            return partitionIdsByTableId.keySet().stream().parallel()
                    .allMatch(this::isTableFinished);
        }

        public boolean isTableFinished(String tableId) {
            TableProperties properties = propertiesProvider.getById(tableId);
            StateStore stateStore = stateStoreProvider.getStateStore(properties);
            Set<String> leafPartitionIds = getLeafPartitionIds(stateStore);
            List<String> unsplit = partitionIdsByTableId.get(tableId).stream()
                    .filter(leafPartitionIds::contains)
                    .collect(Collectors.toUnmodifiableList());
            LOGGER.info("Found unsplit partitions in table {}: {}", properties.getId(), unsplit);
            return unsplit.isEmpty();
        }
    }

    private static Set<String> getLeafPartitionIds(StateStore stateStore) {
        try {
            return stateStore.getLeafPartitions().stream()
                    .map(Partition::getId)
                    .collect(Collectors.toSet());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }
}
