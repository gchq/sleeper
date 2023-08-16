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
import sleeper.systemtest.drivers.ingest.IngestStatusStoreDriver;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.instance.SystemTestParameters;
import sleeper.systemtest.drivers.util.TestContext;

public class SystemTestIngestReporting {
    private final SleeperInstanceContext instance;
    private final SystemTestClients clients;
    private final SystemTestParameters parameters;

    public SystemTestIngestReporting(SleeperInstanceContext instance, SystemTestClients clients, SystemTestParameters parameters) {
        this.instance = instance;
        this.clients = clients;
        this.parameters = parameters;
    }

    public SystemTestIngestReporting clearTasks() {
        statusStoresDriver().tasks().clear();
        return this;
    }

    public SystemTestIngestReporting clearJobs() {
        statusStoresDriver().jobs().clear();
        return this;
    }

    public void printTasksAndJobs(TestContext testContext) {
        reportsDriver().printReports(testContext);
    }

    private IngestStatusStoreDriver statusStoresDriver() {
        return new IngestStatusStoreDriver(clients.getDynamoDB(), instance.getInstanceProperties());
    }

    private IngestReportsDriver reportsDriver() {
        return new IngestReportsDriver(clients.getDynamoDB(), clients.getSqs(), clients.getEmr(), instance, parameters);
    }
}
