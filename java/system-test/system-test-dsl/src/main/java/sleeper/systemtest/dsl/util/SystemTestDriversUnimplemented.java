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
import sleeper.systemtest.dsl.metrics.SystemTestMetrics;
import sleeper.systemtest.dsl.partitioning.PartitionSplittingDriver;
import sleeper.systemtest.dsl.python.SystemTestPythonApi;
import sleeper.systemtest.dsl.query.ClearQueryResultsDriver;
import sleeper.systemtest.dsl.query.QueryAllTablesDriver;
import sleeper.systemtest.dsl.reporting.SystemTestReporting;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.dsl.sourcedata.GeneratedIngestSourceFilesDriver;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesDriver;
import sleeper.systemtest.dsl.sourcedata.SystemTestCluster;

public class SystemTestDriversUnimplemented implements SystemTestDrivers {
    @Override
    public SystemTestDeploymentDriver systemTestDeployment(SystemTestParameters parameters) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SleeperInstanceDriver instance(SystemTestParameters parameters) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SleeperInstanceTablesDriver tables(SystemTestParameters parameters) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GeneratedIngestSourceFilesDriver generatedSourceFiles(SystemTestParameters parameters, DeployedSystemTestResources systemTest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IngestSourceFilesDriver sourceFiles(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionSplittingDriver partitionSplitting(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DirectIngestDriver directIngest(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IngestByQueueDriver ingestByQueue(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DirectBulkImportDriver directEmrServerless(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IngestBatcherDriver ingestBatcher(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InvokeIngestTasksDriver invokeIngestTasks(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public WaitForJobs waitForIngest(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public WaitForJobs waitForBulkImport(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryAllTablesDriver queryByQueue(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryAllTablesDriver directQuery(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClearQueryResultsDriver clearQueryResults(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompactionDriver compaction(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public WaitForJobs waitForCompaction(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SystemTestReporting reporting(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SystemTestMetrics metrics(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SystemTestReports.SystemTestBuilder reportsForExtension(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SystemTestCluster systemTestCluster(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SystemTestPythonApi pythonApi(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PurgeQueueDriver purgeQueueDriver(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }
}
