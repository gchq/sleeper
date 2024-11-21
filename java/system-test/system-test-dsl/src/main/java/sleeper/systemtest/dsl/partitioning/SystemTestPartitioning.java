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

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.statestore.StateStore;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Map.entry;

public class SystemTestPartitioning {

    private final SystemTestInstanceContext instance;
    private final PartitionSplittingDriver splittingDriver;
    private final PollWithRetriesDriver pollDriver;

    public SystemTestPartitioning(SystemTestContext context) {
        SystemTestDrivers drivers = context.instance().adminDrivers();
        instance = context.instance();
        splittingDriver = drivers.partitionSplitting(context);
        pollDriver = drivers.pollWithRetries();
    }

    public void split() {
        WaitForPartitionSplitting wait = WaitForPartitionSplitting.forCurrentPartitionsNeedingSplitting(instance);
        splittingDriver.splitPartitions();
        try {
            wait.pollUntilFinished(instance, pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(5), Duration.ofMinutes(1)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public PartitionTree tree() {
        return tree(instance.getStateStore());
    }

    public Map<String, PartitionTree> treeByTable() {
        return instance.streamTableProperties()
                .map(properties -> entry(
                        instance.getTestTableName(properties),
                        tree(instance.getStateStore(properties))))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private PartitionTree tree(StateStore stateStore) {
        return new PartitionTree(allPartitions(stateStore));
    }

    private List<Partition> allPartitions(StateStore stateStore) {
        return stateStore.getAllPartitions();
    }

    public void setPartitions(PartitionTree tree) {
        instance.getStateStore().initialise(tree.getAllPartitions());
    }
}
