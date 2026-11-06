/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.systemtest.dsl.statestore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.table.TableStatus;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;
import sleeper.systemtest.dsl.util.StreamUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class StateStoreDsl {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreDsl.class);

    private final SystemTestContext context;
    private final StateStoreCommitterDriver driver;
    private final StateStoreCommitterLogsDriver logsDriver;
    private final StateStoreAdminDriver adminDriver;
    private final PollWithRetriesDriver pollDriver;

    public StateStoreDsl(SystemTestContext context) {
        this.context = context;
        SystemTestDrivers adminRoleDrivers = context.instance().adminRoleDrivers();
        driver = adminRoleDrivers.stateStoreCommitter(context);
        logsDriver = adminRoleDrivers.stateStoreCommitterLogs(context);
        adminDriver = adminRoleDrivers.stateStoreAdmin(context);
        pollDriver = adminRoleDrivers.pollWithRetries();
    }

    public StateStoreFakeCommitsDsl fakeCommits() {
        return new StateStoreFakeCommitsDsl(context, driver, logsDriver, pollDriver);
    }

    public double commitsPerSecondForTable() {
        TableStatus table = context.instance().getTableStatus();
        double commitsPerSecond = commitsPerSecondByTableId()
                .getOrDefault(table.getTableUniqueId(), 0.0);
        LOGGER.info("Found commits per second for table {}: {}", table, commitsPerSecond);
        return commitsPerSecond;
    }

    public Map<String, Double> commitsPerSecondByTable() {
        Map<String, Double> byTableId = commitsPerSecondByTableId();
        SystemTestInstanceContext instance = context.instance();
        return instance.streamTableProperties()
                .collect(toMap(
                        table -> instance.getTableDescriptor(table),
                        table -> {
                            Double commitsPerSecond = byTableId.getOrDefault(table.get(TABLE_ID), 0.0);
                            LOGGER.info("Found commits per second for table {}: {}", table.getStatus(), commitsPerSecond);
                            return commitsPerSecond;
                        }));
    }

    private Map<String, Double> commitsPerSecondByTableId() {
        return logsDriver.getLogsInPeriod(context.reporting().getRecordingStartTime(), Instant.now())
                .computeOverallCommitsPerSecondByTableId(
                        context.instance().streamTableProperties()
                                .map(table -> table.get(TABLE_ID))
                                .collect(toSet()));
    }

    public StateStoreDsl invokeSnapshotCreation() {
        adminDriver.triggerCreateSnapshots(
                context.instance().streamTableProperties().map(TableProperties::getStatus));
        return this;
    }

    public StateStoreDsl invokeSnapshotCreationForTablesByDescriptor(Stream<String> descriptors) {
        adminDriver.triggerCreateSnapshots(context.instance().streamTableStatusByDescriptors(descriptors));
        return this;
    }

    public StateStoreDsl invokeSnapshotDeletionForTablesByDescriptor(Stream<String> descriptors) {
        adminDriver.triggerDeleteSnapshots(context.instance().streamTableStatusByDescriptors(descriptors));
        return this;
    }

    public StateStoreDsl invokeTransactionDeletionForTablesByDescriptor(Stream<String> descriptors) {
        adminDriver.triggerDeleteTransactions(context.instance().streamTableStatusByDescriptors(descriptors));
        return this;
    }

    public StateStoreDsl invokeSnapshotDeletionForAllInterleavedWithFakeTables(List<String> fakeTables) {
        adminDriver.triggerDeleteSnapshots(StreamUtils.interleave(
                context.instance().streamTableProperties().map(TableProperties::getStatus),
                fakeTables.stream().map(fake -> TableStatus.uniqueIdAndName(fake, fake, false))));
        return this;
    }

    public StateStoreDsl invokeTransactionDeletionForAllInterleavedWithFakeTables(List<String> fakeTables) {
        adminDriver.triggerDeleteTransactions(StreamUtils.interleave(
                context.instance().streamTableProperties().map(TableProperties::getStatus),
                fakeTables.stream().map(fake -> TableStatus.uniqueIdAndName(fake, fake, false))));
        return this;
    }

    public StateStoreDsl waitForFilesSnapshot(Predicate<AllReferencesToAllFiles> condition) {
        snapshotWaits().waitForFilesSnapshot(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(1), Duration.ofMinutes(2)), condition);
        return this;
    }

    public StateStoreDsl waitForPartitionsSnapshot(Predicate<PartitionTree> condition) {
        snapshotWaits().waitForPartitionsSnapshot(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(1), Duration.ofMinutes(2)), condition);
        return this;
    }

    public AllReferencesToAllFiles waitForFilesSnapshot(PollWithRetries intervalAndPollingTimeout, Predicate<AllReferencesToAllFiles> condition) {
        return snapshotWaits().waitForFilesSnapshot(intervalAndPollingTimeout, condition);
    }

    public PartitionTree waitForPartitionsSnapshot(PollWithRetries intervalAndPollingTimeout, Predicate<PartitionTree> condition) {
        return snapshotWaits().waitForPartitionsSnapshot(intervalAndPollingTimeout, condition);
    }

    public StateStoreDsl waitForFilesSnapshotsToDecreaseTo(int number) {
        snapshotWaits().waitForFilesSnapshotCountDecrease(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(1), Duration.ofMinutes(2)), number);
        return this;
    }

    public StateStoreDsl waitForPartitionsSnapshotsToDecreaseTo(int number) {
        snapshotWaits().waitForPartitionsSnapshotCountDecrease(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(1), Duration.ofMinutes(2)), number);
        return this;
    }

    public StateStoreDsl waitForNoFileTransactions() {
        try {
            pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(1), Duration.ofMinutes(2))
                    .pollUntil("file transactions are deleted",
                            () -> fileTransactions().count() == 0);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return this;
    }

    public StateStoreDsl waitForNoPartitionTransactions() {
        try {
            pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(1), Duration.ofMinutes(2))
                    .pollUntil("partition transactions are deleted",
                            () -> partitionTransactions().count() == 0);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return this;
    }

    private WaitForSnapshot snapshotWaits() {
        return new WaitForSnapshot(context.instance(), adminDriver);
    }

    private Stream<TransactionLogEntry> fileTransactions() {
        return adminDriver.loadAllFileTransactions(
                context.instance().getInstanceProperties(), context.instance().getTableProperties());
    }

    private Stream<TransactionLogEntry> partitionTransactions() {
        return adminDriver.loadAllPartitionTransactions(
                context.instance().getInstanceProperties(), context.instance().getTableProperties());
    }

    public void addUnreadableFilesSnapshot() {
        adminDriver.addUnreadableFilesSnapshot(
                context.instance().getInstanceProperties(), context.instance().getTableProperties());
    }

}
