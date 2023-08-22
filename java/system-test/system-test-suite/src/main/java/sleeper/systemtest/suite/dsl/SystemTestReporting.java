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

import sleeper.systemtest.drivers.ingest.IngestReportsDriver;
import sleeper.systemtest.drivers.instance.ReportingContext;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.util.TestContext;

public class SystemTestReporting {

    private final SleeperInstanceContext instance;
    private final SystemTestClients clients;
    private final ReportingContext context;

    public SystemTestReporting(SleeperInstanceContext instance, SystemTestClients clients, ReportingContext context) {
        this.instance = instance;
        this.clients = clients;
        this.context = context;
    }

    public void startRecording() {
        context.startRecording();
    }

    public void printIngestTasksAndJobs(TestContext testContext) {
        context.print(testContext,
                new IngestReportsDriver(clients.getDynamoDB(), clients.getSqs(), clients.getEmr(), instance)
                        .tasksAndJobsReport());
    }
}
