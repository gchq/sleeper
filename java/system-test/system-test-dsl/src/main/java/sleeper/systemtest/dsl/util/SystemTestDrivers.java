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

package sleeper.systemtest.dsl.util;

import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.compaction.CompactionDriver;
import sleeper.systemtest.dsl.ingest.DirectBulkImportDriver;
import sleeper.systemtest.dsl.ingest.DirectIngestDriver;
import sleeper.systemtest.dsl.ingest.IngestBatcherDriver;
import sleeper.systemtest.dsl.ingest.IngestByQueueDriver;
import sleeper.systemtest.dsl.ingest.InvokeIngestTasksDriver;
import sleeper.systemtest.dsl.instance.DeployedSystemTestResources;
import sleeper.systemtest.dsl.instance.SleeperInstanceDriver;
import sleeper.systemtest.dsl.instance.SleeperInstanceTablesDriver;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentDriver;
import sleeper.systemtest.dsl.instance.SystemTestParameters;
import sleeper.systemtest.dsl.metrics.TableMetricsDriver;
import sleeper.systemtest.dsl.partitioning.PartitionSplittingDriver;
import sleeper.systemtest.dsl.python.SystemTestPythonApi;
import sleeper.systemtest.dsl.query.ClearQueryResultsDriver;
import sleeper.systemtest.dsl.query.QueryAllTablesDriver;
import sleeper.systemtest.dsl.reporting.CompactionReportsDriver;
import sleeper.systemtest.dsl.reporting.IngestReportsDriver;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.dsl.sourcedata.GeneratedIngestSourceFilesDriver;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesDriver;
import sleeper.systemtest.dsl.sourcedata.SystemTestCluster;

public interface SystemTestDrivers {

    SystemTestDeploymentDriver systemTestDeployment(SystemTestParameters parameters);

    SleeperInstanceDriver instance(SystemTestParameters parameters);

    SleeperInstanceTablesDriver tables(SystemTestParameters parameters);

    GeneratedIngestSourceFilesDriver generatedSourceFiles(SystemTestParameters parameters, DeployedSystemTestResources systemTest);

    IngestSourceFilesDriver sourceFiles(SystemTestContext context);

    PartitionSplittingDriver partitionSplitting(SystemTestContext context);

    DirectIngestDriver directIngest(SystemTestContext context);

    IngestByQueueDriver ingestByQueue(SystemTestContext context);

    DirectBulkImportDriver directEmrServerless(SystemTestContext context);

    IngestBatcherDriver ingestBatcher(SystemTestContext context);

    InvokeIngestTasksDriver invokeIngestTasks(SystemTestContext context);

    WaitForJobs waitForIngest(SystemTestContext context);

    WaitForJobs waitForBulkImport(SystemTestContext context);

    QueryAllTablesDriver queryByQueue(SystemTestContext context);

    QueryAllTablesDriver directQuery(SystemTestContext context);

    ClearQueryResultsDriver clearQueryResults(SystemTestContext context);

    CompactionDriver compaction(SystemTestContext context);

    WaitForJobs waitForCompaction(SystemTestContext context);

    IngestReportsDriver ingestReports(SystemTestContext context);

    CompactionReportsDriver compactionReports(SystemTestContext context);

    TableMetricsDriver tableMetrics(SystemTestContext context);

    SystemTestReports.SystemTestBuilder reportsForExtension(SystemTestContext context);

    SystemTestCluster systemTestCluster(SystemTestContext context);

    SystemTestPythonApi pythonApi(SystemTestContext context);

    PurgeQueueDriver purgeQueueDriver(SystemTestContext context);
}
