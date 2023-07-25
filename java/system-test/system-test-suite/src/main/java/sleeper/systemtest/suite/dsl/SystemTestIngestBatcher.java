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
import sleeper.systemtest.drivers.ingest.IngestByQueueDriver;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.instance.SystemTestParameters;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static sleeper.ingest.batcher.IngestBatcher.batchIngestMode;

public class SystemTestIngestBatcher {
    private final SystemTestIngest ingest;
    private final SleeperInstanceContext instance;
    private final IngestBatcherDriver driver;
    private final String sourceBucketName;
    private final Set<String> createdJobIds = new HashSet<>();

    public SystemTestIngestBatcher(SystemTestIngest ingest, SystemTestParameters parameters,
                                   SleeperInstanceContext instance, IngestBatcherDriver driver) {
        this.ingest = ingest;
        this.instance = instance;
        this.driver = driver;
        this.sourceBucketName = parameters.buildSourceBucketName();
    }

    public SystemTestIngestBatcher sendSourceFiles(String... filenames) throws InterruptedException {
        driver.sendFiles(instance.getInstanceProperties(), instance.getTableProperties(),
                sourceBucketName, List.of(filenames));
        return this;
    }

    public SystemTestIngestBatcher invoke() {
        createdJobIds.addAll(driver.invokeGetJobIds());
        return this;
    }

    public void waitForJobs() throws InterruptedException {
        waitForJobs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(10)));
    }

    public void waitForJobs(PollWithRetries pollUntilJobsFinished) throws InterruptedException {
        IngestByQueueDriver driver = ingest.byQueueDriver();
        BatchIngestMode mode = batchIngestMode(instance.getTableProperties()).orElseThrow();
        if (BatchIngestMode.STANDARD_INGEST.equals(mode)) {
            driver.invokeStandardIngestTasks();
        }
        driver.waitForJobs(createdJobIds, pollUntilJobsFinished);
    }

    public void clearStore() {
        driver.clearStore();
    }
}
