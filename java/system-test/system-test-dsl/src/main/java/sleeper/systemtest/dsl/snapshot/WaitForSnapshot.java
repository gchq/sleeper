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
package sleeper.systemtest.dsl.snapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.PartitionTree;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.transactionlog.StateStoreFiles;
import sleeper.core.statestore.transactionlog.StateStorePartitions;
import sleeper.core.statestore.transactionlog.TransactionLogSnapshot;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.Optional;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toUnmodifiableList;

public class WaitForSnapshot {
    public static final Logger LOGGER = LoggerFactory.getLogger(WaitForSnapshot.class);

    private final SystemTestInstanceContext instance;
    private final SnapshotsDriver driver;

    public WaitForSnapshot(SystemTestInstanceContext instance, SnapshotsDriver driver) {
        this.instance = instance;
        this.driver = driver;
    }

    public AllReferencesToAllFiles waitForFilesSnapshot(PollWithRetries poll, Predicate<AllReferencesToAllFiles> condition) throws InterruptedException {
        LOGGER.info("Waiting for files snapshot");
        return poll.queryUntil("files snapshot is present", this::loadLatestFilesSnapshot, matches(condition))
                .orElseThrow();
    }

    public PartitionTree waitForPartitionsSnapshot(PollWithRetries poll, Predicate<PartitionTree> condition) throws InterruptedException {
        LOGGER.info("Waiting for partitions snapshot");
        return poll.queryUntil("partitions snapshot is present", this::loadLatestPartitionsSnapshot, matches(condition))
                .orElseThrow();
    }

    private Optional<AllReferencesToAllFiles> loadLatestFilesSnapshot() {
        Optional<TransactionLogSnapshot> snapshotOpt = driver.loadLatestFilesSnapshot(
                instance.getInstanceProperties(), instance.getTableProperties());
        if (!snapshotOpt.isPresent()) {
            LOGGER.info("Found no files snapshot");
        }
        return snapshotOpt.map(WaitForSnapshot::readFiles);
    }

    private Optional<PartitionTree> loadLatestPartitionsSnapshot() {
        Optional<TransactionLogSnapshot> snapshotOpt = driver.loadLatestPartitionsSnapshot(
                instance.getInstanceProperties(), instance.getTableProperties());
        if (!snapshotOpt.isPresent()) {
            LOGGER.info("Found no partitions snapshot");
        }
        return snapshotOpt.map(WaitForSnapshot::readPartitions);
    }

    private static AllReferencesToAllFiles readFiles(TransactionLogSnapshot snapshot) {
        StateStoreFiles state = snapshot.getState();
        LOGGER.info("Found {} files in snapshot", state.referencedAndUnreferenced().size());
        return new AllReferencesToAllFiles(state.referencedAndUnreferenced(), false);
    }

    private static PartitionTree readPartitions(TransactionLogSnapshot snapshot) {
        StateStorePartitions state = snapshot.getState();
        LOGGER.info("Found {} partitions in snapshot", state.all().size());
        return new PartitionTree(state.all().stream().collect(toUnmodifiableList()));
    }

    private static <T> Predicate<Optional<T>> matches(Predicate<T> condition) {
        return opt -> opt.isPresent() && condition.test(opt.get());
    }
}
