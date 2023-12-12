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

package sleeper.systemtest.suite.dsl;

import sleeper.compaction.strategy.impl.BasicCompactionStrategy;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.drivers.compaction.CompactionDriver;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.util.WaitForJobsDriver;
import sleeper.systemtest.suite.fixtures.SystemTestClients;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;

public class SystemTestCompaction {

    private final SleeperInstanceContext instance;
    private final SystemTestClients clients;
    private List<String> lastJobIds;

    public SystemTestCompaction(SleeperInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.clients = clients;
    }

    public SystemTestCompaction createJobs() {
        lastJobIds = driver().createJobsGetIds();
        return this;
    }

    public SystemTestCompaction forceCreateJobs() {
        lastJobIds = driver().forceCreateJobsGetIds();
        return this;
    }

    public SystemTestCompaction splitAndCompactFiles() throws InterruptedException {
        createJobs().invokeSplittingTasks(1).waitForJobs();
        instance.updateTableProperties(Map.of(
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "1"));
        instance.getTablePropertiesProvider().clearCache();
        forceCreateJobs().invokeStandardTasks(1).waitForJobs(
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(5), Duration.ofMinutes(30)));
        instance.unsetTableProperties(List.of(
                COMPACTION_STRATEGY_CLASS,
                COMPACTION_FILES_BATCH_SIZE));
        instance.getTablePropertiesProvider().clearCache();
        return this;
    }

    public SystemTestCompaction invokeStandardTasks(int expectedTasks) throws InterruptedException {
        driver().invokeTasks(CompactionDriver.Type.STANDARD, expectedTasks);
        return this;
    }

    public SystemTestCompaction invokeSplittingTasks(int expectedTasks) throws InterruptedException {
        driver().invokeTasks(CompactionDriver.Type.SPLITTING, expectedTasks);
        return this;
    }

    public SystemTestCompaction waitForJobs() throws InterruptedException {
        jobsDriver().waitForJobs(lastJobIds);
        return this;
    }

    public SystemTestCompaction waitForJobs(PollWithRetries poll) throws InterruptedException {
        jobsDriver().waitForJobs(lastJobIds, poll);
        return this;
    }

    private CompactionDriver driver() {
        return new CompactionDriver(instance, clients.getLambda(), clients.getDynamoDB(), clients.getSqs());
    }

    private WaitForJobsDriver jobsDriver() {
        return WaitForJobsDriver.forCompaction(instance, clients.getDynamoDB());
    }
}
