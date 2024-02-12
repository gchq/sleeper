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

import sleeper.systemtest.dsl.compaction.SystemTestCompaction;
import sleeper.systemtest.dsl.ingest.SystemTestIngest;
import sleeper.systemtest.dsl.instance.SleeperInstanceContext;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentContext;
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

public interface SystemTestDrivers {

    SystemTestDeploymentContext getSystemTestContext();

    SleeperInstanceContext getInstanceContext();

    IngestSourceFilesContext getSourceFilesContext();

    ReportingContext getReportingContext();

    GeneratedIngestSourceFilesDriver generatedSourceFilesDriver();

    SystemTestSourceFiles sourceFiles();

    SystemTestPartitioning partitioning();

    SystemTestIngest ingest();

    SystemTestQuery query();

    SystemTestCompaction compaction();

    SystemTestReporting reporting();

    SystemTestReports.SystemTestBuilder reportsForExtension();

    SystemTestCluster systemTestCluster();

    SystemTestPythonApi pythonApi();

    PurgeQueueDriver purgeQueueDriver();
}
