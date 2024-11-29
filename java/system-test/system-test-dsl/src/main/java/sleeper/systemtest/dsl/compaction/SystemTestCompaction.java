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
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.time.Duration;
import java.util.List;

public class SystemTestCompaction {

    private final CompactionDriver driver;
    private final PollWithRetriesDriver pollDriver;
    private final WaitForCompactionJobCreation waitForJobCreation;
    private final WaitForJobs waitForJobs;
    private List<String> lastJobIds;

    public SystemTestCompaction(SystemTestContext context) {
        SystemTestDrivers drivers = context.instance().adminDrivers();
        driver = drivers.compaction(context);
        pollDriver = drivers.pollWithRetries();
        waitForJobCreation = new WaitForCompactionJobCreation(context.instance(), driver);
        waitForJobs = drivers.waitForCompaction(context);
    }

    public SystemTestCompaction createJobs(int expectedJobs) {
        return createJobs(expectedJobs,
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(1), Duration.ofMinutes(1)));
    }

    public SystemTestCompaction createJobs(int expectedJobs, PollWithRetries poll) {
        lastJobIds = waitForJobCreation.createJobsGetIds(expectedJobs, pollDriver.poll(poll), driver::triggerCreateJobs);
        return this;
    }

    public SystemTestCompaction forceCreateJobs(int expectedJobs) {
        lastJobIds = waitForJobCreation.createJobsGetIds(expectedJobs,
                pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(1), Duration.ofSeconds(20)),
                driver::forceCreateJobs);
        return this;
    }

    public SystemTestCompaction splitFilesAndRunJobs(int expectedJobs) {
        forceCreateJobs(expectedJobs).invokeTasks(1).waitForJobsToFinishThenCommit(
                pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(5), Duration.ofMinutes(30)),
                pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(5), Duration.ofMinutes(5)));
        return this;
    }

    public SystemTestCompaction invokeTasks(int expectedTasks) {
        driver.invokeTasks(expectedTasks, pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(10), Duration.ofMinutes(3)));
        return this;
    }

    public SystemTestCompaction forceStartTasks(int expectedTasks) {
        driver.forceStartTasks(expectedTasks, pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(10), Duration.ofMinutes(3)));
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

    public void scaleToZero() {
        driver.scaleToZero();
    }
}
