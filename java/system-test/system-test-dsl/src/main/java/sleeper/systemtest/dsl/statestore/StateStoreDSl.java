/*
 * Copyright 2022-2025 Crown Copyright
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
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.table.TableStatus;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.snapshot.SnapshotsDriver;
import sleeper.systemtest.dsl.snapshot.WaitForSnapshot;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;

import java.time.Instant;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class StateStoreDSl {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreDSl.class);

    private final SystemTestContext context;
    private final StateStoreCommitterDriver driver;
    private final StateStoreCommitterLogsDriver logsDriver;
    private final SnapshotsDriver snapshotsDriver;
    private final PollWithRetriesDriver pollDriver;

    public StateStoreDSl(SystemTestContext context) {
        this.context = context;
        SystemTestDrivers adminDrivers = context.instance().adminDrivers();
        driver = adminDrivers.stateStoreCommitter(context);
        logsDriver = adminDrivers.stateStoreCommitterLogs(context);
        snapshotsDriver = adminDrivers.snapshots();
        pollDriver = adminDrivers.pollWithRetries();
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
                        table -> instance.getTestTableName(table),
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

    public AllReferencesToAllFiles waitForFilesSnapshot(PollWithRetries intervalAndPollingTimeout, Predicate<AllReferencesToAllFiles> condition) throws InterruptedException {
        return new WaitForSnapshot(context.instance(), snapshotsDriver)
                .waitForFilesSnapshot(intervalAndPollingTimeout, condition);
    }

    public PartitionTree waitForPartitionsSnapshot(PollWithRetries intervalAndPollingTimeout, Predicate<PartitionTree> condition) throws InterruptedException {
        return new WaitForSnapshot(context.instance(), snapshotsDriver)
                .waitForPartitionsSnapshot(intervalAndPollingTimeout, condition);
    }

}
