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

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.drivers.compaction.SplittingCompactionDriver;
import sleeper.systemtest.drivers.compaction.StandardCompactionDriver;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.util.WaitForJobsDriver;

import java.util.List;

public class SystemTestCompaction {

    private final SleeperInstanceContext instance;
    private final SystemTestClients clients;

    public SystemTestCompaction(SleeperInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.clients = clients;
    }

    public void runSplitting() throws InterruptedException {
        new SplittingCompactionDriver(instance, clients.getLambda(), clients.getSqs(), clients.getDynamoDB())
                .runSplittingCompaction();
    }

    public void runStandard(PollWithRetries pollUntilJobsFinished) throws InterruptedException {
        StandardCompactionDriver driver = new StandardCompactionDriver(instance, clients.getLambda(), clients.getDynamoDB());
        List<String> jobIds = driver.createJobsGetIds();
        driver.invokeTasks(jobIds.size());
        WaitForJobsDriver.forCompaction(instance, clients.getDynamoDB())
                .waitForJobs(jobIds, pollUntilJobsFinished);
    }
}
