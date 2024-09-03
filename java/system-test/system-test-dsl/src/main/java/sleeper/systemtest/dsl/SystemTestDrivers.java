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

package sleeper.systemtest.dsl;

import sleeper.systemtest.dsl.compaction.CompactionDriver;
import sleeper.systemtest.dsl.gc.GarbageCollectionDriver;
import sleeper.systemtest.dsl.ingest.DirectBulkImportDriver;
import sleeper.systemtest.dsl.ingest.DirectIngestDriver;
import sleeper.systemtest.dsl.ingest.IngestBatcherDriver;
import sleeper.systemtest.dsl.ingest.IngestByAnyQueueDriver;
import sleeper.systemtest.dsl.ingest.IngestByQueue;
import sleeper.systemtest.dsl.ingest.IngestLocalFileByAnyQueueDriver;
import sleeper.systemtest.dsl.ingest.InvokeIngestTasksDriver;
import sleeper.systemtest.dsl.instance.AssumeAdminRoleDriver;
import sleeper.systemtest.dsl.instance.DeployedSystemTestResources;
import sleeper.systemtest.dsl.instance.SleeperInstanceDriver;
import sleeper.systemtest.dsl.instance.SleeperTablesDriver;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentDriver;
import sleeper.systemtest.dsl.instance.SystemTestParameters;
import sleeper.systemtest.dsl.metrics.TableMetricsDriver;
import sleeper.systemtest.dsl.partitioning.PartitionSplittingDriver;
import sleeper.systemtest.dsl.python.PythonQueryTypesDriver;
import sleeper.systemtest.dsl.query.ClearQueryResultsDriver;
import sleeper.systemtest.dsl.query.QueryAllTablesDriver;
import sleeper.systemtest.dsl.reporting.CompactionReportsDriver;
import sleeper.systemtest.dsl.reporting.IngestReportsDriver;
import sleeper.systemtest.dsl.reporting.PartitionReportDriver;
import sleeper.systemtest.dsl.snapshot.SnapshotsDriver;
import sleeper.systemtest.dsl.sourcedata.DataGenerationTasksDriver;
import sleeper.systemtest.dsl.sourcedata.GeneratedIngestSourceFilesDriver;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesDriver;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterDriver;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterLogsDriver;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;
import sleeper.systemtest.dsl.util.PurgeQueueDriver;
import sleeper.systemtest.dsl.util.WaitForJobs;

/**
 * The interface for the system test DSL to interact with Sleeper and the deployed environment. Where
 * {@link SleeperSystemTest} defines the language for interacting with Sleeper, the implementation is defined
 * by drivers that can be accessed through an implementation of this interface.
 */
public interface SystemTestDrivers {

    SystemTestDeploymentDriver systemTestDeployment(SystemTestParameters parameters);

    SleeperInstanceDriver instance(SystemTestParameters parameters);

    AssumeAdminRoleDriver assumeAdminRole();

    SleeperTablesDriver tables(SystemTestParameters parameters);

    GeneratedIngestSourceFilesDriver generatedSourceFiles(SystemTestParameters parameters, DeployedSystemTestResources systemTest);

    IngestSourceFilesDriver sourceFiles(SystemTestContext context);

    StateStoreCommitterDriver stateStoreCommitter(SystemTestContext context);

    StateStoreCommitterLogsDriver stateStoreCommitterLogs(SystemTestContext context);

    PartitionSplittingDriver partitionSplitting(SystemTestContext context);

    DirectIngestDriver directIngest(SystemTestContext context);

    IngestByQueue ingestByQueue(SystemTestContext context);

    DirectBulkImportDriver directEmrServerless(SystemTestContext context);

    IngestBatcherDriver ingestBatcher(SystemTestContext context);

    InvokeIngestTasksDriver invokeIngestTasks(SystemTestContext context);

    WaitForJobs waitForIngest(SystemTestContext context);

    WaitForJobs waitForBulkImport(SystemTestContext context);

    QueryAllTablesDriver queryByQueue(SystemTestContext context);

    QueryAllTablesDriver directQuery(SystemTestContext context);

    QueryAllTablesDriver queryByWebSocket(SystemTestContext context);

    ClearQueryResultsDriver clearQueryResults(SystemTestContext context);

    CompactionDriver compaction(SystemTestContext context);

    WaitForJobs waitForCompaction(SystemTestContext context);

    GarbageCollectionDriver garbageCollection(SystemTestContext context);

    DataGenerationTasksDriver dataGenerationTasks(SystemTestContext context);

    IngestByAnyQueueDriver pythonIngest(SystemTestContext context);

    IngestLocalFileByAnyQueueDriver pythonIngestLocalFile(SystemTestContext context);

    IngestByAnyQueueDriver pythonBulkImport(SystemTestContext context);

    PythonQueryTypesDriver pythonQuery(SystemTestContext context);

    IngestReportsDriver ingestReports(SystemTestContext context);

    CompactionReportsDriver compactionReports(SystemTestContext context);

    PartitionReportDriver partitionReports(SystemTestContext context);

    TableMetricsDriver tableMetrics(SystemTestContext context);

    default PollWithRetriesDriver pollWithRetries() {
        return PollWithRetriesDriver.realWaits();
    }

    PurgeQueueDriver purgeQueues(SystemTestContext context);

    SnapshotsDriver snapshots();
}
