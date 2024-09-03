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

package sleeper.systemtest.dsl.python;

import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.ingest.IngestByAnyQueueDriver;
import sleeper.systemtest.dsl.ingest.IngestLocalFileByAnyQueueDriver;
import sleeper.systemtest.dsl.ingest.InvokeIngestTasksDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.nio.file.Path;

public class SystemTestPythonApi {
    private final SystemTestInstanceContext instance;
    private final IngestByAnyQueueDriver ingestDriver;
    private final IngestLocalFileByAnyQueueDriver ingestLocalFileDriver;
    private final IngestByAnyQueueDriver bulkImportDriver;
    private final InvokeIngestTasksDriver tasksDriver;
    private final WaitForJobs waitForIngest;
    private final WaitForJobs waitForBulkImport;
    private final PythonQueryTypesDriver queryDriver;

    public SystemTestPythonApi(SystemTestContext context) {
        instance = context.instance();
        SystemTestDrivers drivers = instance.adminDrivers();
        ingestDriver = drivers.pythonIngest(context);
        ingestLocalFileDriver = drivers.pythonIngestLocalFile(context);
        bulkImportDriver = drivers.pythonBulkImport(context);
        tasksDriver = drivers.invokeIngestTasksNew(context);
        waitForIngest = drivers.waitForIngest(context);
        waitForBulkImport = drivers.waitForBulkImport(context);
        queryDriver = drivers.pythonQuery(context);
    }

    public SystemTestPythonIngest ingestByQueue() {
        return new SystemTestPythonIngest(ingestDriver, ingestLocalFileDriver, tasksDriver, waitForIngest);
    }

    public SystemTestPythonBulkImport bulkImport() {
        return new SystemTestPythonBulkImport(bulkImportDriver, waitForBulkImport);
    }

    public SystemTestPythonQuery query(Path outputDir) {
        return new SystemTestPythonQuery(instance, queryDriver, outputDir);
    }
}
