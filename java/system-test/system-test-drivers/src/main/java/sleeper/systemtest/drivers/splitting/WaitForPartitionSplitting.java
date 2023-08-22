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

package sleeper.systemtest.drivers.splitting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.PollWithRetries;
import sleeper.splitter.FindPartitionToSplitResult;
import sleeper.splitter.FindPartitionsToSplit;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class WaitForPartitionSplitting {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForPartitionSplitting.class);

    private static final long POLL_INTERVAL_MILLIS = 5000;
    private static final int MAX_POLLS = 12;
    private final List<String> toSplitIds;

    private WaitForPartitionSplitting(List<FindPartitionToSplitResult> toSplit) {
        this.toSplitIds = toSplit.stream()
                .map(FindPartitionToSplitResult::getPartition)
                .map(Partition::getId)
                .collect(Collectors.toUnmodifiableList());
    }

    public static WaitForPartitionSplitting forCurrentPartitionsNeedingSplitting(
            TableProperties tableProperties, StateStore stateStore) throws StateStoreException {
        return new WaitForPartitionSplitting(
                FindPartitionsToSplit.getResults(tableProperties, stateStore));
    }

    public void pollUntilFinished(StateStore stateStore) throws InterruptedException {
        LOGGER.info("Waiting for splits, expecting partitions to be split: {}", toSplitIds);
        PollWithRetries.intervalAndMaxPolls(POLL_INTERVAL_MILLIS, MAX_POLLS)
                .pollUntil("partition splits finished", () -> isSplitFinished(stateStore));
    }

    public boolean isSplitFinished(StateStore stateStore) {
        Set<String> leafPartitionIds = getLeafPartitionIds(stateStore);
        List<String> unsplit = toSplitIds.stream()
                .filter(leafPartitionIds::contains)
                .collect(Collectors.toUnmodifiableList());
        LOGGER.info("Found unsplit partitions: {}", unsplit);
        return unsplit.isEmpty();
    }

    private Set<String> getLeafPartitionIds(StateStore stateStore) {
        try {
            return stateStore.getLeafPartitions().stream()
                    .map(Partition::getId)
                    .collect(Collectors.toSet());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }
}
