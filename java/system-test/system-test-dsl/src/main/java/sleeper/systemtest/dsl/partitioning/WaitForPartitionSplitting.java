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

package sleeper.systemtest.dsl.partitioning;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.PollWithRetries;
import sleeper.splitter.find.FindPartitionToSplitResult;
import sleeper.splitter.find.FindPartitionsToSplit;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class WaitForPartitionSplitting {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForPartitionSplitting.class);

    private final Map<String, Set<String>> partitionIdsByTableId;

    private WaitForPartitionSplitting(List<FindPartitionToSplitResult> toSplit) {
        partitionIdsByTableId = toSplit.stream()
                .collect(Collectors.groupingBy(FindPartitionToSplitResult::getTableId,
                        Collectors.mapping(split -> split.getPartition().getId(), Collectors.toSet())));
    }

    public static WaitForPartitionSplitting forCurrentPartitionsNeedingSplitting(SystemTestInstanceContext instance) {
        return forCurrentPartitionsNeedingSplitting(instance.streamTableProperties(), instance::getStateStore);
    }

    public static WaitForPartitionSplitting forCurrentPartitionsNeedingSplitting(
            Stream<TableProperties> tablePropertiesStream, Function<TableProperties, StateStore> getStateStore) {
        return new WaitForPartitionSplitting(getResults(tablePropertiesStream, getStateStore));
    }

    public void pollUntilFinished(SystemTestInstanceContext instance, PollWithRetries poll) throws InterruptedException {
        LOGGER.info("Waiting for splits, expecting partitions to be split: {}", partitionIdsByTableId);
        poll.pollUntil("partition splits finished", () -> isSplitFinished(instance));
    }

    private boolean isSplitFinished(SystemTestInstanceContext instance) {
        return partitionIdsByTableId.keySet().stream().parallel()
                .allMatch(tableId -> isSplitFinished(instance, tableId));
    }

    private boolean isSplitFinished(SystemTestInstanceContext instance, String tableId) {
        TableProperties properties = instance.getTablePropertiesByDeployedId(tableId).orElseThrow();
        StateStore stateStore = instance.getStateStore(properties);
        return isSplitFinished(properties, stateStore);
    }

    public boolean isSplitFinished(TableProperties properties, StateStore stateStore) {
        Set<String> leafPartitionIds = getLeafPartitionIds(stateStore);
        List<String> unsplit = partitionIdsByTableId.getOrDefault(properties.get(TABLE_ID), Set.of()).stream()
                .filter(leafPartitionIds::contains)
                .collect(Collectors.toUnmodifiableList());
        LOGGER.info("Found unsplit partitions in table {}: {}", properties.getStatus(), unsplit);
        return unsplit.isEmpty();
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

    private static List<FindPartitionToSplitResult> getResults(
            Stream<TableProperties> streamTableProperties, Function<TableProperties, StateStore> getStateStore) {
        return streamTableProperties.parallel()
                .flatMap(properties -> {
                    try {
                        return FindPartitionsToSplit.getResults(properties, getStateStore.apply(properties)).stream();
                    } catch (StateStoreException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toUnmodifiableList());
    }
}
