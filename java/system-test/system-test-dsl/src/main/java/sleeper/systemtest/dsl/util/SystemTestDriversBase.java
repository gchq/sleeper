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
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.compaction.CompactionDriver;
import sleeper.systemtest.dsl.gc.GarbageCollectionDriver;
import sleeper.systemtest.dsl.ingest.DirectBulkImportDriver;
import sleeper.systemtest.dsl.ingest.DirectIngestDriver;
import sleeper.systemtest.dsl.ingest.IngestBatcherDriver;
import sleeper.systemtest.dsl.ingest.IngestByAnyQueueDriver;
import sleeper.systemtest.dsl.ingest.IngestByQueue;
import sleeper.systemtest.dsl.ingest.IngestLocalFileByAnyQueueDriver;
import sleeper.systemtest.dsl.ingest.InvokeIngestTasksDriver;
import sleeper.systemtest.dsl.metrics.TableMetricsDriver;
import sleeper.systemtest.dsl.partitioning.PartitionSplittingDriver;
import sleeper.systemtest.dsl.python.PythonQueryTypesDriver;
import sleeper.systemtest.dsl.query.ClearQueryResultsDriver;
import sleeper.systemtest.dsl.query.QueryAllTablesDriver;
import sleeper.systemtest.dsl.reporting.CompactionReportsDriver;
import sleeper.systemtest.dsl.reporting.IngestReportsDriver;
import sleeper.systemtest.dsl.reporting.PartitionReportDriver;
import sleeper.systemtest.dsl.sourcedata.DataGenerationTasksDriver;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesDriver;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterDriver;

public abstract class SystemTestDriversBase implements SystemTestDrivers {

    @Override
    public StateStoreCommitterDriver stateStoreCommitter(SystemTestContext context) {
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
    public IngestByQueue ingestByQueue(SystemTestContext context) {
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
    public GarbageCollectionDriver garbageCollection(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DataGenerationTasksDriver dataGenerationTasks(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IngestByAnyQueueDriver pythonIngest(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IngestLocalFileByAnyQueueDriver pythonIngestLocalFile(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IngestByAnyQueueDriver pythonBulkImport(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PythonQueryTypesDriver pythonQuery(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IngestReportsDriver ingestReports(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompactionReportsDriver compactionReports(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionReportDriver partitionReports(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableMetricsDriver tableMetrics(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }
}
