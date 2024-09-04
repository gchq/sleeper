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
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesContext;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.nio.file.Path;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;

public class SystemTestIngest {
    private final SystemTestContext context;
    private final SystemTestDrivers baseDrivers;
    private final SystemTestDrivers adminDrivers;

    public SystemTestIngest(SystemTestContext context, SystemTestDrivers baseDrivers) {
        this.context = context;
        this.baseDrivers = baseDrivers;
        this.adminDrivers = context.instance().adminDrivers();
    }

    public SystemTestIngest setType(SystemTestIngestType type) {
        type.applyTo(instance());
        return this;
    }

    public SystemTestIngestBatcher batcher() {
        return new SystemTestIngestBatcher(context, adminDrivers);
    }

    public SystemTestDirectIngest direct(Path tempDir) {
        return new SystemTestDirectIngest(instance(), adminDrivers.directIngest(context), tempDir);
    }

    public SystemTestIngestToStateStore toStateStore() {
        return new SystemTestIngestToStateStore(instance(), sourceFiles());
    }

    public SystemTestIngestByQueue byQueue() {
        return new SystemTestIngestByQueue(sourceFiles(), ingestByQueue(), INGEST_JOB_QUEUE_URL, tasksDriver(), waitForIngest(), pollDriver());
    }

    public SystemTestIngestByQueue bulkImportByQueue() {
        return new SystemTestIngestByQueue(sourceFiles(), ingestByQueue(), BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL, noTasksDriverForBulkImport(), waitForBulkImport(), pollDriver());
    }

    public SystemTestDirectBulkImport directEmrServerless() {
        return new SystemTestDirectBulkImport(
                instance(), sourceFiles(), baseDrivers.directEmrServerless(context), waitForBulkImport());
    }

    private SystemTestInstanceContext instance() {
        return context.instance();
    }

    private IngestSourceFilesContext sourceFiles() {
        return context.sourceFiles();
    }

    private IngestByQueue ingestByQueue() {
        return adminDrivers.ingestByQueue(context);
    }

    private InvokeIngestTasksDriver noTasksDriverForBulkImport() {
        return () -> {
            throw new IllegalArgumentException("Bulk import does not require tasks to be invoked.");
        };
    }

    private InvokeIngestTasksDriver tasksDriver() {
        return adminDrivers.invokeIngestTasks(context);
    }

    private WaitForJobs waitForIngest() {
        return adminDrivers.waitForIngest(context);
    }

    private WaitForJobs waitForBulkImport() {
        return adminDrivers.waitForBulkImport(context);
    }

    private PollWithRetriesDriver pollDriver() {
        return adminDrivers.pollWithRetries();
    }
}
