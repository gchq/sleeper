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

package sleeper.systemtest.dsl.compaction;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesContext;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;
import sleeper.systemtest.dsl.util.WaitForJobs;
import sleeper.systemtest.dsl.util.WaitForTasks;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;

public class SystemTestCompaction {

    private final SystemTestInstanceContext instance;
    private final IngestSourceFilesContext sourceFiles;
    private final CompactionDriver driver;
    private final CompactionDriver baseDriver;
    private final PollWithRetriesDriver pollDriver;
    private final WaitForCompactionJobCreation waitForJobCreation;
    private final WaitForJobs waitForJobs;
    private List<String> lastJobIds;

    public SystemTestCompaction(SystemTestContext context, SystemTestDrivers baseDrivers) {
        this.instance = context.instance();
        this.sourceFiles = context.sourceFiles();
        SystemTestDrivers drivers = instance.adminDrivers();
        driver = drivers.compaction(context);
        pollDriver = drivers.pollWithRetries();
        waitForJobCreation = new WaitForCompactionJobCreation(instance, driver);
        waitForJobs = drivers.waitForCompaction(context);
        // Use base driver to drain compaction queue as admin role does not have permission to do this
        baseDriver = baseDrivers.compaction(context);
    }

    public SystemTestCompaction createJobs(int expectedJobs) {
        return createJobs(expectedJobs,
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(1), Duration.ofMinutes(1)));
    }

    public SystemTestCompaction createJobs(int expectedJobs, PollWithRetries poll) {
        lastJobIds = waitForJobCreation.createJobsGetIds(expectedJobs, pollDriver.poll(poll), driver::triggerCreateJobs);
        return this;
    }

    public SystemTestCompaction putTableOnlineWaitForJobCreation(int expectedJobs) {
        return putTablesOnlineWaitForJobCreation(expectedJobs);
    }

    public SystemTestCompaction putTablesOnlineWaitForJobCreation(int expectedJobs) {
        return putTablesOnlineWaitForJobCreation(expectedJobs,
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(15), Duration.ofMinutes(2)));
    }

    public SystemTestCompaction putTableOnlineWaitForJobCreation(int expectedJobs, PollWithRetries poll) {
        return putTablesOnlineWaitForJobCreation(expectedJobs, poll);
    }

    public SystemTestCompaction putTablesOnlineWaitForJobCreation(int expectedJobs, PollWithRetries poll) {
        lastJobIds = waitForJobCreation.createJobsGetIds(expectedJobs, pollDriver.poll(poll),
                () -> instance.updateTableProperties(Map.of(TABLE_ONLINE, "true")));
        return this;
    }

    public SystemTestCompaction forceCreateJobs(int expectedJobs) {
        lastJobIds = waitForJobCreation.createJobsGetIds(expectedJobs,
                pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(1), Duration.ofMinutes(1)),
                driver::forceCreateJobs);
        return this;
    }

    public SystemTestCompaction splitFilesAndRunJobs(int expectedJobs) {
        forceCreateJobs(expectedJobs).waitForTasks(1).waitForJobsToFinishThenCommit(
                pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(5), Duration.ofMinutes(30)),
                pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(5), Duration.ofMinutes(5)));
        return this;
    }

    public SystemTestCompaction waitForTasks(int expectedTasks) {
        new WaitForTasks(driver.getJobStatusStore())
                .waitUntilNumTasksStartedAJob(expectedTasks, lastJobIds,
                        pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(10), Duration.ofMinutes(3)));
        return this;
    }

    public SystemTestCompaction waitForJobs() {
        waitForJobs.waitForJobs(lastJobIds);
        return this;
    }

    public SystemTestCompaction waitForJobs(PollWithRetries poll) {
        waitForJobs.waitForJobs(lastJobIds, poll);
        return this;
    }

    public SystemTestCompaction waitForJobsToFinishThenCommit(
            PollWithRetries pollUntilFinished, PollWithRetries pollUntilCommitted) {
        waitForJobs.waitForJobs(lastJobIds, pollUntilFinished, pollUntilCommitted);
        return this;
    }

    public FoundCompactionJobs drainJobsQueueForWholeInstance() {
        return FoundCompactionJobs.from(sourceFiles, baseDriver.drainJobsQueueForWholeInstance());
    }

    public void scaleToZero() {
        driver.scaleToZero();
    }
}
