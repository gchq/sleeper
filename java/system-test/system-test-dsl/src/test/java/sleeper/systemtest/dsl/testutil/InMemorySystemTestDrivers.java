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

package sleeper.systemtest.dsl.testutil;

import sleeper.ingest.batcher.testutil.InMemoryIngestBatcherStore;
import sleeper.query.runner.recordretrieval.InMemoryDataStore;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.compaction.CompactionDriver;
import sleeper.systemtest.dsl.ingest.DirectIngestDriver;
import sleeper.systemtest.dsl.ingest.IngestBatcherDriver;
import sleeper.systemtest.dsl.ingest.IngestByQueue;
import sleeper.systemtest.dsl.ingest.InvokeIngestTasksDriver;
import sleeper.systemtest.dsl.instance.DeployedSystemTestResources;
import sleeper.systemtest.dsl.instance.SleeperInstanceDriver;
import sleeper.systemtest.dsl.instance.SleeperTablesDriver;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentDriver;
import sleeper.systemtest.dsl.instance.SystemTestParameters;
import sleeper.systemtest.dsl.query.QueryAllTablesDriver;
import sleeper.systemtest.dsl.sourcedata.GeneratedIngestSourceFilesDriver;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryCompaction;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryDirectIngestDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryDirectQueryDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryGeneratedIngestSourceFilesDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryIngestBatcherDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryIngestByQueue;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryQueryByQueueDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemorySleeperInstanceDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemorySleeperTablesDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemorySourceFilesDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemorySystemTestDeploymentDriver;
import sleeper.systemtest.dsl.util.PurgeQueueDriver;
import sleeper.systemtest.dsl.util.SystemTestDriversBase;
import sleeper.systemtest.dsl.util.WaitForJobs;

public class InMemorySystemTestDrivers extends SystemTestDriversBase {

    private final SystemTestDeploymentDriver systemTestDeploymentDriver = new InMemorySystemTestDeploymentDriver();
    private final InMemorySleeperTablesDriver tablesDriver = new InMemorySleeperTablesDriver();
    private final SleeperInstanceDriver instanceDriver = new InMemorySleeperInstanceDriver(tablesDriver);
    private final InMemoryDataStore sourceFiles = new InMemoryDataStore();
    private final InMemoryDataStore data = new InMemoryDataStore();
    private final InMemoryIngestBatcherStore batcherStore = new InMemoryIngestBatcherStore();
    private final InMemoryIngestByQueue ingestByQueue = new InMemoryIngestByQueue(sourceFiles, data);
    private final InMemoryCompaction compaction = new InMemoryCompaction(data);

    @Override
    public SystemTestDeploymentDriver systemTestDeployment(SystemTestParameters parameters) {
        return systemTestDeploymentDriver;
    }

    @Override
    public SleeperInstanceDriver instance(SystemTestParameters parameters) {
        return instanceDriver;
    }

    @Override
    public SleeperTablesDriver tables(SystemTestParameters parameters) {
        return tablesDriver;
    }

    @Override
    public IngestSourceFilesDriver sourceFiles(SystemTestContext context) {
        return new InMemorySourceFilesDriver(sourceFiles);
    }

    @Override
    public GeneratedIngestSourceFilesDriver generatedSourceFiles(SystemTestParameters parameters, DeployedSystemTestResources systemTest) {
        return new InMemoryGeneratedIngestSourceFilesDriver();
    }

    @Override
    public DirectIngestDriver directIngest(SystemTestContext context) {
        return new InMemoryDirectIngestDriver(context.instance(), data);
    }

    @Override
    public IngestByQueue ingestByQueue(SystemTestContext context) {
        return new IngestByQueue(context.instance(), ingestByQueue.byQueueDriver());
    }

    @Override
    public InvokeIngestTasksDriver invokeIngestTasks(SystemTestContext context) {
        return ingestByQueue.tasksDriver();
    }

    @Override
    public WaitForJobs waitForIngest(SystemTestContext context) {
        return ingestByQueue.waitForIngest(context);
    }

    @Override
    public WaitForJobs waitForBulkImport(SystemTestContext context) {
        return ingestByQueue.waitForBulkImport(context);
    }

    @Override
    public IngestBatcherDriver ingestBatcher(SystemTestContext context) {
        return new InMemoryIngestBatcherDriver(context.instance(), batcherStore, ingestByQueue);
    }

    @Override
    public CompactionDriver compaction(SystemTestContext context) {
        return compaction.driver(context.instance());
    }

    @Override
    public WaitForJobs waitForCompaction(SystemTestContext context) {
        return compaction.waitForJobs(context);
    }

    @Override
    public QueryAllTablesDriver directQuery(SystemTestContext context) {
        return InMemoryDirectQueryDriver.allTablesDriver(context.instance(), data);
    }

    @Override
    public QueryAllTablesDriver queryByQueue(SystemTestContext context) {
        return InMemoryQueryByQueueDriver.allTablesDriver(context.instance(), data);
    }

    @Override
    public PurgeQueueDriver purgeQueues(SystemTestContext context) {
        return properties -> {
        };
    }
}
