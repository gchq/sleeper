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

package sleeper.systemtest.suite.dsl.ingest;

import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.drivers.ingest.IngestByQueueDriver;
import sleeper.systemtest.drivers.ingest.IngestSourceFilesDriver;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.util.WaitForJobsDriver;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;

public class SystemTestIngestByQueue {

    private final SleeperInstanceContext instance;
    private final IngestSourceFilesDriver sourceFiles;
    private final IngestByQueueDriver driver;
    private final WaitForJobsDriver waitForJobsDriver;
    private final List<String> sentJobIds = new ArrayList<>();

    public SystemTestIngestByQueue(SleeperInstanceContext instance,
                                   IngestSourceFilesDriver sourceFiles,
                                   IngestByQueueDriver driver,
                                   WaitForJobsDriver waitForJobsDriver) {
        this.instance = instance;
        this.sourceFiles = sourceFiles;
        this.driver = driver;
        this.waitForJobsDriver = waitForJobsDriver;
    }

    public SystemTestIngestByQueue sendSourceFiles(String... files) {
        return sendSourceFiles(INGEST_JOB_QUEUE_URL, files);
    }

    public SystemTestIngestByQueue sendSourceFiles(InstanceProperty queueProperty, String... files) {
        sentJobIds.add(driver.sendJobGetId(queueProperty, sourceFiles(files)));
        return this;
    }

    public SystemTestIngestByQueue sendSourceFilesToAllTables(String... files) {
        sentJobIds.addAll(instance.streamTableNames().parallel()
                .map(tableName -> driver.sendJobGetId(INGEST_JOB_QUEUE_URL, tableName, sourceFiles(files)))
                .collect(Collectors.toList()));
        return this;
    }

    public SystemTestIngestByQueue invokeTask() throws InterruptedException {
        driver.invokeStandardIngestTask();
        return this;
    }

    public void waitForJobs() throws InterruptedException {
        waitForJobsDriver.waitForJobs(sentJobIds);
    }

    public void waitForJobs(PollWithRetries pollWithRetries) throws InterruptedException {
        waitForJobsDriver.waitForJobs(sentJobIds, pollWithRetries);
    }

    private List<String> sourceFiles(String... files) {
        return sourceFiles.getIngestJobFilesInBucket(Stream.of(files));
    }
}
