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

package sleeper.systemtest.suite.dsl.reports;

import sleeper.systemtest.drivers.compaction.CompactionReportsDriver;
import sleeper.systemtest.drivers.ingest.IngestReportsDriver;
import sleeper.systemtest.drivers.instance.ReportingContext;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.partitioning.PartitionReportDriver;
import sleeper.systemtest.drivers.util.TestContext;
import sleeper.systemtest.suite.fixtures.SystemTestClients;

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

    public void printPartitionStatus(TestContext testContext) {
        context.print(testContext, partitions().statusReport());
    }

    public void printIngestTasksAndJobs(TestContext testContext) {
        context.print(testContext, ingest().tasksAndJobsReport());
    }

    public void printIngestJobs(TestContext testContext) {
        context.print(testContext, ingest().jobsReport());
    }

    public SystemTestIngestJobsReport ingestJobs() {
        return new SystemTestIngestJobsReport(ingest().jobs(context));
    }

    public void printCompactionTasksAndJobs(TestContext testContext) {
        context.print(testContext, compaction().tasksAndJobsReport());
    }

    public SystemTestCompactionJobsReport compactionJobs() {
        return new SystemTestCompactionJobsReport(compaction().jobs(context));
    }

    private PartitionReportDriver partitions() {
        return new PartitionReportDriver(instance);
    }

    private IngestReportsDriver ingest() {
        return new IngestReportsDriver(clients.getDynamoDB(), clients.getSqs(), clients.getEmr(), instance);
    }

    private CompactionReportsDriver compaction() {
        return new CompactionReportsDriver(clients.getDynamoDB(), instance);
    }
}
