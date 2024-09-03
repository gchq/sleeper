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

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesContext;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.util.List;
import java.util.stream.Stream;

public class SystemTestIngestBatcher {
    private final IngestSourceFilesContext sourceFiles;
    private final IngestBatcherDriver driver;
    private final InvokeIngestTasksDriver tasksDriver;
    private final WaitForJobs waitForIngest;
    private final WaitForJobs waitForBulkImport;
    private Result lastInvokeResult;

    public SystemTestIngestBatcher(SystemTestContext context, SystemTestDrivers drivers) {
        this.sourceFiles = context.sourceFiles();
        this.driver = drivers.ingestBatcher(context);
        this.tasksDriver = drivers.invokeIngestTasks(context);
        this.waitForIngest = drivers.waitForIngest(context);
        this.waitForBulkImport = drivers.waitForBulkImport(context);
    }

    public SystemTestIngestBatcher sendSourceFiles(String... filenames) {
        driver.sendFiles(sourceFiles.getIngestJobFilesInBucket(Stream.of(filenames)));
        return this;
    }

    public SystemTestIngestBatcher invoke() {
        lastInvokeResult = new Result(driver.invokeGetJobIds());
        return this;
    }

    public SystemTestIngestBatcher invokeStandardIngestTask() {
        tasksDriver.invokeTasksForCurrentInstance().invokeUntilOneTaskStartedAJob(getInvokeResult().createdJobIds);
        return this;
    }

    public SystemTestIngestBatcher waitForIngestJobs() {
        waitForIngest.waitForJobs(getInvokeResult().createdJobIds);
        return this;
    }

    public SystemTestIngestBatcher waitForBulkImportJobs(PollWithRetries pollWithRetries) {
        waitForBulkImport.waitForJobs(getInvokeResult().createdJobIds, pollWithRetries);
        return this;
    }

    public Result getInvokeResult() {
        if (lastInvokeResult == null) {
            throw new IllegalStateException("Batcher has not been invoked");
        }
        return lastInvokeResult;
    }

    public void clearStore() {
        driver.clearStore();
    }

    public static class Result {
        private final List<String> createdJobIds;

        public Result(List<String> createdJobIds) {
            this.createdJobIds = createdJobIds;
        }

        public int numJobsCreated() {
            return createdJobIds.size();
        }
    }
}
