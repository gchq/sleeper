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

package sleeper.systemtest.drivers.util;

import sleeper.systemtest.drivers.compaction.AwsCompactionDriver;
import sleeper.systemtest.drivers.compaction.AwsCompactionReportsDriver;
import sleeper.systemtest.drivers.ingest.AwsDataGenerationTasksDriver;
import sleeper.systemtest.drivers.ingest.AwsDirectIngestDriver;
import sleeper.systemtest.drivers.ingest.AwsIngestBatcherDriver;
import sleeper.systemtest.drivers.ingest.AwsIngestByQueueDriver;
import sleeper.systemtest.drivers.ingest.AwsIngestReportsDriver;
import sleeper.systemtest.drivers.ingest.AwsInvokeIngestTasksDriver;
import sleeper.systemtest.drivers.ingest.AwsPurgeQueueDriver;
import sleeper.systemtest.drivers.ingest.DirectEmrServerlessDriver;
import sleeper.systemtest.drivers.instance.AwsSleeperInstanceDriver;
import sleeper.systemtest.drivers.instance.AwsSleeperInstanceTablesDriver;
import sleeper.systemtest.drivers.instance.AwsSystemTestDeploymentDriver;
import sleeper.systemtest.drivers.metrics.AwsTableMetricsDriver;
import sleeper.systemtest.drivers.partitioning.AwsPartitionReportDriver;
import sleeper.systemtest.drivers.partitioning.AwsPartitionSplittingDriver;
import sleeper.systemtest.drivers.python.PythonBulkImportDriver;
import sleeper.systemtest.drivers.python.PythonIngestDriver;
import sleeper.systemtest.drivers.python.PythonIngestLocalFileDriver;
import sleeper.systemtest.drivers.python.PythonQueryDriver;
import sleeper.systemtest.drivers.query.DirectQueryDriver;
import sleeper.systemtest.drivers.query.S3ResultsDriver;
import sleeper.systemtest.drivers.query.SQSQueryDriver;
import sleeper.systemtest.drivers.sourcedata.AwsGeneratedIngestSourceFilesDriver;
import sleeper.systemtest.drivers.sourcedata.AwsIngestSourceFilesDriver;
import sleeper.systemtest.dsl.compaction.SystemTestCompaction;
import sleeper.systemtest.dsl.ingest.IngestByQueue;
import sleeper.systemtest.dsl.ingest.SystemTestIngest;
import sleeper.systemtest.dsl.instance.DeployedSleeperInstances;
import sleeper.systemtest.dsl.instance.SleeperInstanceDriver;
import sleeper.systemtest.dsl.instance.SleeperInstanceTablesDriver;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentContext;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
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
import sleeper.systemtest.dsl.util.PurgeQueueDriver;
import sleeper.systemtest.dsl.util.SystemTestDrivers;

import java.nio.file.Path;

public class AwsSystemTestDrivers implements SystemTestDrivers {
    private final SystemTestClients clients = new SystemTestClients();
    private final SystemTestParameters parameters;
    private final SystemTestDeploymentContext systemTestContext;
    private final SystemTestInstanceContext instanceContext;
    private final IngestSourceFilesContext sourceFilesContext;
    private final ReportingContext reportingContext;

    public AwsSystemTestDrivers(SystemTestParameters parameters) {
        this.parameters = parameters;
        systemTestContext = new SystemTestDeploymentContext(
                parameters, new AwsSystemTestDeploymentDriver(parameters, clients));
        SleeperInstanceDriver instanceDriver = new AwsSleeperInstanceDriver(parameters, clients);
        SleeperInstanceTablesDriver tablesDriver = new AwsSleeperInstanceTablesDriver(clients);
        DeployedSleeperInstances deployedInstances = new DeployedSleeperInstances(
                parameters, systemTestContext, instanceDriver, tablesDriver);
        instanceContext = new SystemTestInstanceContext(parameters, deployedInstances, instanceDriver, tablesDriver);
        sourceFilesContext = new IngestSourceFilesContext(systemTestContext, instanceContext);
        reportingContext = new ReportingContext(parameters);
    }

    @Override
    public SystemTestDeploymentContext getSystemTestContext() {
        return systemTestContext;
    }

    @Override
    public SystemTestInstanceContext getInstanceContext() {
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
        return new AwsGeneratedIngestSourceFilesDriver(systemTestContext, clients.getS3V2());
    }

    @Override
    public SystemTestSourceFiles sourceFiles() {
        return new SystemTestSourceFiles(instanceContext, sourceFilesContext,
                new AwsIngestSourceFilesDriver(sourceFilesContext));
    }

    @Override
    public SystemTestPartitioning partitioning() {
        return new SystemTestPartitioning(instanceContext,
                new AwsPartitionSplittingDriver(instanceContext, clients.getLambda()));
    }

    @Override
    public SystemTestIngest ingest() {
        return new SystemTestIngest(instanceContext, sourceFilesContext,
                new AwsDirectIngestDriver(instanceContext),
                new IngestByQueue(instanceContext, new AwsIngestByQueueDriver(clients)),
                new DirectEmrServerlessDriver(instanceContext, clients),
                new AwsIngestBatcherDriver(instanceContext, sourceFilesContext, clients),
                new AwsInvokeIngestTasksDriver(instanceContext, clients),
                AwsWaitForJobs.forIngest(instanceContext, clients.getDynamoDB()),
                AwsWaitForJobs.forBulkImport(instanceContext, clients.getDynamoDB()));
    }

    @Override
    public SystemTestQuery query() {
        return new SystemTestQuery(instanceContext,
                SQSQueryDriver.allTablesDriver(instanceContext, clients),
                DirectQueryDriver.allTablesDriver(instanceContext),
                new S3ResultsDriver(instanceContext, clients.getS3()));
    }

    @Override
    public SystemTestCompaction compaction() {
        return new SystemTestCompaction(
                new AwsCompactionDriver(instanceContext, clients),
                AwsWaitForJobs.forCompaction(instanceContext, clients.getDynamoDB()));
    }

    @Override
    public SystemTestReporting reporting() {
        return new SystemTestReporting(reportingContext,
                new AwsIngestReportsDriver(instanceContext, clients),
                new AwsCompactionReportsDriver(instanceContext, clients.getDynamoDB()));
    }

    @Override
    public SystemTestMetrics metrics() {
        return new SystemTestMetrics(new AwsTableMetricsDriver(instanceContext, reportingContext, clients));
    }

    @Override
    public SystemTestReports.SystemTestBuilder reportsForExtension() {
        return SystemTestReports.builder(reportingContext,
                new AwsPartitionReportDriver(instanceContext),
                new AwsIngestReportsDriver(instanceContext, clients),
                new AwsCompactionReportsDriver(instanceContext, clients.getDynamoDB()));
    }

    @Override
    public SystemTestCluster systemTestCluster() {
        return new SystemTestCluster(systemTestContext,
                new AwsDataGenerationTasksDriver(systemTestContext, instanceContext, clients.getEcs()),
                new IngestByQueue(instanceContext, new AwsIngestByQueueDriver(clients)),
                new AwsGeneratedIngestSourceFilesDriver(systemTestContext, clients.getS3V2()),
                new AwsInvokeIngestTasksDriver(instanceContext, clients),
                AwsWaitForJobs.forIngest(instanceContext, clients.getDynamoDB()),
                AwsWaitForJobs.forBulkImport(instanceContext, clients.getDynamoDB()));
    }

    @Override
    public SystemTestPythonApi pythonApi() {
        Path pythonDir = parameters.getPythonDirectory();
        return new SystemTestPythonApi(instanceContext,
                new PythonIngestDriver(instanceContext, pythonDir),
                new PythonIngestLocalFileDriver(instanceContext, pythonDir),
                new PythonBulkImportDriver(instanceContext, pythonDir),
                new AwsInvokeIngestTasksDriver(instanceContext, clients),
                AwsWaitForJobs.forIngest(instanceContext, clients.getDynamoDB()),
                AwsWaitForJobs.forBulkImport(instanceContext, clients.getDynamoDB()),
                new PythonQueryDriver(instanceContext, pythonDir));
    }

    @Override
    public PurgeQueueDriver purgeQueueDriver() {
        return new AwsPurgeQueueDriver(instanceContext, clients.getSqs());
    }
}
