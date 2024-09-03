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

import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesContext;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class SystemTestIngestByQueue {

    private final IngestSourceFilesContext sourceFiles;
    private final IngestByQueue ingest;
    private final InstanceProperty defaultQueueProperty;
    private final InvokeIngestTasksDriver invokeTasksDriver;
    private final WaitForJobs waitForJobs;
    private final PollWithRetriesDriver pollDriver;
    private final List<String> sentJobIds = new ArrayList<>();

    public SystemTestIngestByQueue(
            IngestSourceFilesContext sourceFiles, IngestByQueue ingest, InstanceProperty defaultQueueProperty,
            InvokeIngestTasksDriver invokeTasksDriver, WaitForJobs waitForJobs, PollWithRetriesDriver pollDriver) {
        this.sourceFiles = sourceFiles;
        this.ingest = ingest;
        this.defaultQueueProperty = defaultQueueProperty;
        this.invokeTasksDriver = invokeTasksDriver;
        this.waitForJobs = waitForJobs;
        this.pollDriver = pollDriver;
    }

    public SystemTestIngestByQueue sendSourceFiles(String... files) {
        return sendSourceFiles(defaultQueueProperty, files);
    }

    public SystemTestIngestByQueue sendSourceFiles(InstanceProperty queueProperty, String... files) {
        sentJobIds.add(ingest.sendJobGetId(queueProperty, sourceFiles(files)));
        return this;
    }

    public SystemTestIngestByQueue sendSourceFilesToAllTables(String... files) {
        sentJobIds.addAll(ingest.sendJobToAllTablesGetIds(defaultQueueProperty, sourceFiles(files)));
        return this;
    }

    public SystemTestIngestByQueue invokeTask() {
        invokeTasksDriver.invokeTasksForCurrentInstance().invokeUntilOneTaskStartedAJob(sentJobIds, pollDriver);
        return this;
    }

    public void waitForJobs() {
        waitForJobs.waitForJobs(sentJobIds);
    }

    public void waitForJobs(PollWithRetries pollWithRetries) {
        waitForJobs.waitForJobs(sentJobIds, pollWithRetries);
    }

    private List<String> sourceFiles(String... files) {
        return sourceFiles.getIngestJobFilesInBucket(Stream.of(files));
    }
}
