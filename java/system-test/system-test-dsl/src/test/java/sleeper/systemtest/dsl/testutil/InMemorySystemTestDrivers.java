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
import sleeper.systemtest.dsl.compaction.SystemTestCompaction;
import sleeper.systemtest.dsl.ingest.SystemTestIngest;
import sleeper.systemtest.dsl.instance.SleeperInstanceContext;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentContext;
import sleeper.systemtest.dsl.instance.SystemTestParameters;
import sleeper.systemtest.dsl.metrics.SystemTestMetrics;
import sleeper.systemtest.dsl.partitioning.SystemTestPartitioning;
import sleeper.systemtest.dsl.python.SystemTestPythonApi;
import sleeper.systemtest.dsl.query.SystemTestQuery;
import sleeper.systemtest.dsl.reporting.ReportingContext;
import sleeper.systemtest.dsl.reporting.SystemTestReporting;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.dsl.sourcedata.GeneratedIngestSourceFilesDriver;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesContext;
import sleeper.systemtest.dsl.sourcedata.SystemTestCluster;
import sleeper.systemtest.dsl.sourcedata.SystemTestSourceFiles;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryDirectIngestDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryGeneratedIngestSourceFilesDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryQueryDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemorySleeperInstanceDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemorySleeperInstanceTablesDriver;
import sleeper.systemtest.dsl.testutil.drivers.InMemorySystemTestDeploymentDriver;
import sleeper.systemtest.dsl.util.PurgeQueueDriver;
import sleeper.systemtest.dsl.util.SystemTestDrivers;

public class InMemorySystemTestDrivers implements SystemTestDrivers {
    private final SystemTestDeploymentContext systemTestContext;
    private final SleeperInstanceContext instanceContext;
    private final IngestSourceFilesContext sourceFilesContext;
    private final ReportingContext reportingContext;
    private final InMemoryDataStore data = new InMemoryDataStore();

    public InMemorySystemTestDrivers(SystemTestParameters parameters) {
        systemTestContext = new SystemTestDeploymentContext(parameters, new InMemorySystemTestDeploymentDriver());
        InMemorySleeperInstanceTablesDriver tablesDriver = new InMemorySleeperInstanceTablesDriver();
        instanceContext = new SleeperInstanceContext(parameters, systemTestContext,
                new InMemorySleeperInstanceDriver(tablesDriver), tablesDriver);
        sourceFilesContext = new IngestSourceFilesContext(systemTestContext, instanceContext);
        reportingContext = new ReportingContext(parameters);
    }

    @Override
    public SystemTestDeploymentContext getSystemTestContext() {
        return systemTestContext;
    }

    @Override
    public SleeperInstanceContext getInstanceContext() {
        return instanceContext;
    }

    @Override
    public IngestSourceFilesContext getSourceFilesContext() {
        return sourceFilesContext;
    }

    @Override
    public ReportingContext getReportingContext() {
        return reportingContext;
    }

    @Override
    public GeneratedIngestSourceFilesDriver generatedSourceFilesDriver() {
        return new InMemoryGeneratedIngestSourceFilesDriver();
    }

    @Override
    public SystemTestSourceFiles sourceFiles() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SystemTestPartitioning partitioning() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SystemTestIngest ingest() {
        return new SystemTestIngest(instanceContext, null,
                new InMemoryDirectIngestDriver(instanceContext, data),
                null, null, null, null, null, null);
    }

    @Override
    public SystemTestQuery query() {
        return new SystemTestQuery(instanceContext, null,
                InMemoryQueryDriver.allTablesDriver(instanceContext, data),
                null);
    }

    @Override
    public SystemTestCompaction compaction() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SystemTestReporting reporting() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SystemTestMetrics metrics() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SystemTestReports.SystemTestBuilder reportsForExtension() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SystemTestCluster systemTestCluster() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SystemTestPythonApi pythonApi() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PurgeQueueDriver purgeQueueDriver() {
        return properties -> {
        };
    }
}
