/*
 * Copyright 2022-2025 Crown Copyright
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

package sleeper.systemtest.dsl.testutil;

import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.bulkexport.BulkExportDriver;
import sleeper.systemtest.dsl.compaction.CompactionDriver;
import sleeper.systemtest.dsl.gc.GarbageCollectionDriver;
import sleeper.systemtest.dsl.ingest.DirectIngestDriver;
import sleeper.systemtest.dsl.ingest.IngestBatcherDriver;
import sleeper.systemtest.dsl.ingest.IngestByQueue;
import sleeper.systemtest.dsl.ingest.IngestTasksDriver;
import sleeper.systemtest.dsl.instance.AssumeAdminRoleDriver;
import sleeper.systemtest.dsl.instance.DataFilesDriver;
import sleeper.systemtest.dsl.instance.DeployedSystemTestResources;
import sleeper.systemtest.dsl.instance.ScheduleRulesDriver;
import sleeper.systemtest.dsl.instance.SleeperInstanceDriver;
import sleeper.systemtest.dsl.instance.SleeperTablesDriver;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentDriver;
import sleeper.systemtest.dsl.instance.SystemTestParameters;
import sleeper.systemtest.dsl.metrics.TableMetricsDriver;
import sleeper.systemtest.dsl.partitioning.PartitionSplittingDriver;
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
import sleeper.systemtest.dsl.testutil.drivers.InMemoryBulkExportDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryDataFilesDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryDataGenerationTasksDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryDirectIngestDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryDirectQueryDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryGarbageCollectionDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryGeneratedIngestSourceFilesDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryIngestBatcherDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryPartitionSplittingDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryQueryByQueueDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryReports;
import sleeper.systemtest.dsl.testutil.drivers.InMemorySourceFilesDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryStateStoreCommitter;
import sleeper.systemtest.dsl.util.NoScheduleRulesDriver;
import sleeper.systemtest.dsl.util.NoSnapshotsDriver;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;
import sleeper.systemtest.dsl.util.PurgeQueueDriver;
import sleeper.systemtest.dsl.util.SystemTestDriversBase;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.util.List;

public class InMemorySystemTestDrivers extends SystemTestDriversBase {

    private final InMemorySystemTestState inMemoryState = new InMemorySystemTestState();

    @Override
    public SystemTestDeploymentDriver systemTestDeployment(SystemTestParameters parameters) {
        return inMemoryState.getSystemTestDeploymentDriver();
    }

    @Override
    public SleeperInstanceDriver instance(SystemTestParameters parameters) {
        return inMemoryState.getInstanceDriver();
    }

    @Override
    public AssumeAdminRoleDriver assumeAdminRole() {
        return properties -> this;
    }

    @Override
    public SleeperTablesDriver tables(SystemTestParameters parameters) {
        return inMemoryState.getTablesDriver();
    }

    @Override
    public IngestSourceFilesDriver sourceFiles(SystemTestContext context) {
        return new InMemorySourceFilesDriver(inMemoryState.getSourceFiles(), inMemoryState.getData(), inMemoryState.getSketches());
    }

    @Override
    public GeneratedIngestSourceFilesDriver generatedSourceFiles(SystemTestParameters parameters, DeployedSystemTestResources systemTest) {
        return new InMemoryGeneratedIngestSourceFilesDriver(inMemoryState.getSourceFiles());
    }

    @Override
    public StateStoreCommitterDriver stateStoreCommitter(SystemTestContext context) {
        return inMemoryState.getStateStoreCommitter().withContext(context);
    }

    @Override
    public StateStoreCommitterLogsDriver stateStoreCommitterLogs(SystemTestContext context) {
        return inMemoryState.getStateStoreCommitter().logsDriver();
    }

    @Override
    public DirectIngestDriver directIngest(SystemTestContext context) {
        return new InMemoryDirectIngestDriver(context.instance(), inMemoryState.getData(), inMemoryState.getSketches());
    }

    @Override
    public IngestByQueue ingestByQueue(SystemTestContext context) {
        return new IngestByQueue(context.instance(), inMemoryState.getIngestByQueue().byQueueDriver(context));
    }

    @Override
    public IngestTasksDriver ingestTasks(SystemTestContext context) {
        return inMemoryState.getIngestByQueue().tasksDriver(context);
    }

    @Override
    public WaitForJobs waitForIngest(SystemTestContext context) {
        return inMemoryState.getIngestByQueue().waitForIngest(context, pollWithRetries());
    }

    @Override
    public WaitForJobs waitForBulkImport(SystemTestContext context) {
        return inMemoryState.getIngestByQueue().waitForBulkImport(context, pollWithRetries());
    }

    @Override
    public IngestBatcherDriver ingestBatcher(SystemTestContext context) {
        return new InMemoryIngestBatcherDriver(context, inMemoryState.getBatcherStore(), inMemoryState.getIngestByQueue(), inMemoryState.getFileSizeBytesForBatcher());
    }

    public void fixSizeOfFilesSeenByBatcherInBytes(long fileSizeBytes) {
        inMemoryState.setFileSizeBytesForBatcher(fileSizeBytes);
    }

    @Override
    public CompactionDriver compaction(SystemTestContext context) {
        return inMemoryState.getCompaction().driver(context.instance());
    }

    @Override
    public WaitForJobs waitForCompaction(SystemTestContext context) {
        return inMemoryState.getCompaction().waitForJobs(context, pollWithRetries());
    }

    @Override
    public GarbageCollectionDriver garbageCollection(SystemTestContext context) {
        return new InMemoryGarbageCollectionDriver(context.instance(), inMemoryState.getData());
    }

    @Override
    public QueryAllTablesDriver directQuery(SystemTestContext context) {
        return InMemoryDirectQueryDriver.allTablesDriver(context.instance(), inMemoryState.getData());
    }

    @Override
    public QueryAllTablesDriver queryByQueue(SystemTestContext context) {
        return InMemoryQueryByQueueDriver.allTablesDriver(context.instance(), inMemoryState.getData());
    }

    @Override
    public QueryAllTablesDriver queryByWebSocket(SystemTestContext context) {
        return InMemoryQueryByQueueDriver.allTablesDriver(context.instance(), inMemoryState.getData());
    }

    @Override
    public PurgeQueueDriver purgeQueues(SystemTestContext context) {
        return properties -> {
        };
    }

    @Override
    public PartitionSplittingDriver partitionSplitting(SystemTestContext context) {
        return new InMemoryPartitionSplittingDriver(context.instance(), inMemoryState.getSketches());
    }

    @Override
    public TableMetricsDriver tableMetrics(SystemTestContext context) {
        return inMemoryState.getMetrics().driver(context);
    }

    @Override
    public CompactionReportsDriver compactionReports(SystemTestContext context) {
        return inMemoryState.getReports().compaction(context.instance());
    }

    @Override
    public IngestReportsDriver ingestReports(SystemTestContext context) {
        return inMemoryState.getReports().ingest(context.instance());
    }

    @Override
    public PartitionReportDriver partitionReports(SystemTestContext context) {
        return inMemoryState.getReports().partitions(context.instance());
    }

    @Override
    public DataGenerationTasksDriver dataGenerationTasks(SystemTestContext context) {
        return new InMemoryDataGenerationTasksDriver(context.instance(), inMemoryState.getData(), inMemoryState.getSketches());
    }

    @Override
    public DataFilesDriver dataFiles(SystemTestContext context) {
        return new InMemoryDataFilesDriver(inMemoryState.getData(), context);
    }

    @Override
    public PollWithRetriesDriver pollWithRetries() {
        return PollWithRetriesDriver.noWaits();
    }

    @Override
    public SnapshotsDriver snapshots() {
        return new NoSnapshotsDriver();
    }

    @Override
    public ScheduleRulesDriver schedules() {
        return new NoScheduleRulesDriver();
    }

    public InMemoryReports reports() {
        return inMemoryState.getReports();
    }

    public InMemoryStateStoreCommitter stateStoreCommitter() {
        return inMemoryState.getStateStoreCommitter();
    }

    @Override
    public BulkExportDriver bulkExport(SystemTestContext context) {
        return new InMemoryBulkExportDriver(inMemoryState.getBulkExport());
    }

    public List<BulkExportQuery> getBulkExportQueueQueries() {
        return inMemoryState.getBulkExport().getQueriesOnQueue();
    }

}
