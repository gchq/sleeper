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

import sleeper.systemtest.dsl.compaction.SystemTestCompaction;
import sleeper.systemtest.dsl.ingest.SystemTestIngest;
import sleeper.systemtest.dsl.instance.SleeperInstanceContext;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentContext;
import sleeper.systemtest.dsl.instance.SystemTestParameters;
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
import sleeper.systemtest.dsl.util.PurgeQueueDriver;
import sleeper.systemtest.dsl.util.SystemTestDrivers;

public class InMemorySystemTestDrivers implements SystemTestDrivers {
    private final SystemTestParameters parameters;
    private final SystemTestDeploymentContext systemTestContext;
    private final SleeperInstanceContext instanceContext;
    private final IngestSourceFilesContext sourceFilesContext;
    private final ReportingContext reportingContext;

    public InMemorySystemTestDrivers(SystemTestParameters parameters) {
        this.parameters = parameters;
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
        return null;
    }

    @Override
    public SystemTestSourceFiles sourceFiles() {
        return null;
    }

    @Override
    public SystemTestPartitioning partitioning() {
        return null;
    }

    @Override
    public SystemTestIngest ingest() {
        return null;
    }

    @Override
    public SystemTestQuery query() {
        return null;
    }

    @Override
    public SystemTestCompaction compaction() {
        return null;
    }

    @Override
    public SystemTestReporting reporting() {
        return null;
    }

    @Override
    public SystemTestReports.SystemTestBuilder reportsForExtension() {
        return null;
    }

    @Override
    public SystemTestCluster systemTestCluster() {
        return null;
    }

    @Override
    public SystemTestPythonApi pythonApi() {
        return null;
    }

    @Override
    public PurgeQueueDriver purgeQueueDriver() {
        return null;
    }
}
