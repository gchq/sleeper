/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.suite.dsl;

import sleeper.configuration.properties.validation.BatchIngestMode;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.drivers.ingest.IngestBatcherDriver;
import sleeper.systemtest.drivers.ingest.IngestSourceFilesContext;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.time.Duration;
import java.util.List;

import static sleeper.ingest.batcher.IngestBatcher.batchIngestMode;

public class SystemTestIngestBatcher {
    private final SystemTestIngest ingest;
    private final SleeperInstanceContext instance;
    private final IngestBatcherDriver driver;
    private final IngestSourceFilesContext sourceFiles;
    private IngestBatcherResult lastInvokeResult;

    public SystemTestIngestBatcher(SystemTestIngest ingest, IngestSourceFilesContext sourceFiles,
                                   SleeperInstanceContext instance, IngestBatcherDriver driver) {
        this.ingest = ingest;
        this.instance = instance;
        this.driver = driver;
        this.sourceFiles = sourceFiles;
    }

    public SystemTestIngestBatcher sendSourceFiles(String... filenames) throws InterruptedException {
        driver.sendFiles(instance.getInstanceProperties(), instance.getTableProperties(),
                sourceFiles.getSourceBucketName(), List.of(filenames));
        return this;
    }

    public SystemTestIngestBatcher invoke() {
        lastInvokeResult = new IngestBatcherResult(driver.invokeGetJobIds());
        return this;
    }

    public SystemTestIngestBatcher waitForJobs() throws InterruptedException {
        return waitForJobs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(10)));
    }

    public SystemTestIngestBatcher waitForJobs(PollWithRetries pollUntilJobsFinished) throws InterruptedException {
        BatchIngestMode mode = batchIngestMode(instance.getTableProperties()).orElseThrow();
        if (BatchIngestMode.STANDARD_INGEST.equals(mode)) {
            ingest.byQueue().invokeTasks();
        }
        ingest.waitForIngestJobsDriver()
                .waitForJobs(getInvokeResult().createdJobIds(), pollUntilJobsFinished);
        return this;
    }

    public IngestBatcherResult getInvokeResult() {
        if (lastInvokeResult == null) {
            throw new IllegalStateException("Batcher has not been invoked");
        }
        return lastInvokeResult;
    }

    public void clearStore() {
        driver.clearStore();
    }
}
