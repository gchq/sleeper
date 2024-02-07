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

import sleeper.systemtest.dsl.instance.SleeperInstanceContext;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesContext;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.nio.file.Path;

public class SystemTestIngest {
    private final SleeperInstanceContext instance;
    private final IngestSourceFilesContext sourceFiles;
    private final DirectIngestDriver directDriver;
    private final IngestByQueue byQueue;
    private final DirectBulkImportDriver directEmrServerlessDriver;
    private final IngestBatcherDriver batcherDriver;
    private final WaitForJobs waitForIngest;
    private final WaitForJobs waitForBulkImport;

    public SystemTestIngest(SleeperInstanceContext instance, IngestSourceFilesContext sourceFiles,
                            DirectIngestDriver directDriver, IngestByQueue byQueue,
                            DirectBulkImportDriver directEmrServerlessDriver, IngestBatcherDriver batcherDriver,
                            WaitForJobs waitForIngest, WaitForJobs waitForBulkImport) {
        this.instance = instance;
        this.sourceFiles = sourceFiles;
        this.directDriver = directDriver;
        this.byQueue = byQueue;
        this.directEmrServerlessDriver = directEmrServerlessDriver;
        this.batcherDriver = batcherDriver;
        this.waitForIngest = waitForIngest;
        this.waitForBulkImport = waitForBulkImport;
    }

    public SystemTestIngest setType(SystemTestIngestType type) {
        type.applyTo(instance);
        return this;
    }

    public SystemTestIngestBatcher batcher() {
        return new SystemTestIngestBatcher(batcherDriver, byQueue, waitForIngest, waitForBulkImport);
    }

    public SystemTestDirectIngest direct(Path tempDir) {
        return new SystemTestDirectIngest(instance, directDriver, tempDir);
    }

    public SystemTestIngestToStateStore toStateStore() {
        return new SystemTestIngestToStateStore(instance, sourceFiles);
    }

    public SystemTestIngestByQueue byQueue() {
        return new SystemTestIngestByQueue(sourceFiles, byQueue, waitForIngest);
    }

    public SystemTestIngestByQueue bulkImportByQueue() {
        return new SystemTestIngestByQueue(sourceFiles, byQueue, waitForBulkImport);
    }

    public SystemTestDirectBulkImport directEmrServerless() {
        return new SystemTestDirectBulkImport(instance, sourceFiles, directEmrServerlessDriver, waitForBulkImport);
    }
}
