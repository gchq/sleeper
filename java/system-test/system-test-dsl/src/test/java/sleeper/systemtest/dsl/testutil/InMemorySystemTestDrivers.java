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

import sleeper.query.runner.recordretrieval.InMemoryDataStore;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.compaction.SystemTestCompaction;
import sleeper.systemtest.dsl.ingest.SystemTestIngest;
import sleeper.systemtest.dsl.instance.DeployedSystemTestResources;
import sleeper.systemtest.dsl.instance.SleeperInstanceDriver;
import sleeper.systemtest.dsl.instance.SleeperInstanceTablesDriver;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentDriver;
import sleeper.systemtest.dsl.instance.SystemTestParameters;
import sleeper.systemtest.dsl.metrics.SystemTestMetrics;
import sleeper.systemtest.dsl.partitioning.PartitionSplittingDriver;
import sleeper.systemtest.dsl.python.SystemTestPythonApi;
import sleeper.systemtest.dsl.query.SystemTestQuery;
import sleeper.systemtest.dsl.reporting.SystemTestReporting;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.dsl.sourcedata.GeneratedIngestSourceFilesDriver;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesDriver;
import sleeper.systemtest.dsl.sourcedata.SystemTestCluster;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryDirectIngestDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryGeneratedIngestSourceFilesDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryQueryDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemorySleeperInstanceDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemorySleeperInstanceTablesDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemorySystemTestDeploymentDriver;
import sleeper.systemtest.dsl.util.PurgeQueueDriver;
import sleeper.systemtest.dsl.util.SystemTestDrivers;

public class InMemorySystemTestDrivers implements SystemTestDrivers {

    private final SystemTestDeploymentDriver systemTestDeploymentDriver = new InMemorySystemTestDeploymentDriver();
    private final InMemorySleeperInstanceTablesDriver tablesDriver = new InMemorySleeperInstanceTablesDriver();
    private final SleeperInstanceDriver instanceDriver = new InMemorySleeperInstanceDriver(tablesDriver);
    private final InMemoryDataStore data = new InMemoryDataStore();

    @Override
    public SystemTestDeploymentDriver systemTestDeployment(SystemTestParameters parameters) {
        return systemTestDeploymentDriver;
    }

    @Override
    public SleeperInstanceDriver instance(SystemTestParameters parameters) {
        return instanceDriver;
    }

    @Override
    public SleeperInstanceTablesDriver tables(SystemTestParameters parameters) {
        return tablesDriver;
    }

    @Override
    public GeneratedIngestSourceFilesDriver generatedSourceFiles(SystemTestParameters parameters, DeployedSystemTestResources systemTest) {
        return new InMemoryGeneratedIngestSourceFilesDriver();
    }

    @Override
    public IngestSourceFilesDriver sourceFilesDriver(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionSplittingDriver partitionSplitting(SystemTestContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SystemTestIngest ingest(SystemTestContext context) {
        return new SystemTestIngest(context.instance(), null,
                new InMemoryDirectIngestDriver(context.instance(), data),
                null, null, null, null, null, null);
    }

    @Override
    public SystemTestQuery query(SystemTestContext context) {
        return new SystemTestQuery(context.instance(), null,
                InMemoryQueryDriver.allTablesDriver(context.instance(), data),
                null);
    }

    @Override
    public SystemTestCompaction compaction(SystemTestContext context) {
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
        return properties -> {
        };
    }
}
