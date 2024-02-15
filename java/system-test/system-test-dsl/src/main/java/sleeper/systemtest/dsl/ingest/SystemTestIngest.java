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

package sleeper.systemtest.dsl.ingest;

import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesContext;
import sleeper.systemtest.dsl.util.SystemTestDrivers;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.nio.file.Path;

public class SystemTestIngest {
    private final SystemTestContext context;
    private final SystemTestDrivers drivers;

    public SystemTestIngest(SystemTestContext context, SystemTestDrivers drivers) {
        this.context = context;
        this.drivers = drivers;
    }

    public SystemTestIngest setType(SystemTestIngestType type) {
        type.applyTo(instance());
        return this;
    }

    public SystemTestIngestBatcher batcher() {
        return new SystemTestIngestBatcher(
                drivers.ingestBatcher(context), tasksDriver(), waitForIngest(), waitForBulkImport());
    }

    public SystemTestDirectIngest direct(Path tempDir) {
        return new SystemTestDirectIngest(instance(), drivers.directIngest(context), tempDir);
    }

    public SystemTestIngestToStateStore toStateStore() {
        return new SystemTestIngestToStateStore(instance(), sourceFiles());
    }

    public SystemTestIngestByQueue byQueue() {
        return new SystemTestIngestByQueue(sourceFiles(), ingestByQueue(), tasksDriver(), waitForIngest());
    }

    public SystemTestIngestByQueue bulkImportByQueue() {
        return new SystemTestIngestByQueue(sourceFiles(), ingestByQueue(), tasksDriver(), waitForBulkImport());
    }

    public SystemTestDirectBulkImport directEmrServerless() {
        return new SystemTestDirectBulkImport(
                instance(), sourceFiles(), drivers.directEmrServerless(context), waitForBulkImport());
    }

    private SystemTestInstanceContext instance() {
        return context.instance();
    }

    private IngestSourceFilesContext sourceFiles() {
        return context.sourceFiles();
    }

    private IngestByQueue ingestByQueue() {
        return drivers.ingestByQueue(context);
    }

    private InvokeIngestTasksDriver tasksDriver() {
        return drivers.invokeIngestTasks(context);
    }

    private WaitForJobs waitForIngest() {
        return drivers.waitForIngest(context);
    }

    private WaitForJobs waitForBulkImport() {
        return drivers.waitForBulkImport(context);
    }
}
