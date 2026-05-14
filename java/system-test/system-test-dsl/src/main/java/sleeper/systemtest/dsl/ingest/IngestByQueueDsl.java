/*
 * Copyright 2022-2026 Crown Copyright
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

import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SentJobsContext;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesContext;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.util.List;
import java.util.stream.Stream;

public class IngestByQueueDsl {

    private final SentJobsContext sentJobs;
    private final IngestSourceFilesContext sourceFiles;
    private final IngestByQueue ingest;
    private final CdkDefinedInstanceProperty defaultQueueProperty;
    private final IngestTasksDriver tasksDriver;
    private final WaitForJobs waitForJobs;
    private final PollWithRetriesDriver pollDriver;

    public IngestByQueueDsl(
            SentJobsContext sentJobs, IngestSourceFilesContext sourceFiles, IngestByQueue ingest,
            CdkDefinedInstanceProperty defaultQueueProperty,
            IngestTasksDriver tasksDriver, WaitForJobs waitForJobs, PollWithRetriesDriver pollDriver) {
        this.sentJobs = sentJobs;
        this.sourceFiles = sourceFiles;
        this.ingest = ingest;
        this.defaultQueueProperty = defaultQueueProperty;
        this.tasksDriver = tasksDriver;
        this.waitForJobs = waitForJobs;
        this.pollDriver = pollDriver;
    }

    public IngestByQueueDsl sendSourceFiles(String... files) {
        return sendSourceFiles(defaultQueueProperty, files);
    }

    public IngestByQueueDsl sendSourceFiles(CdkDefinedInstanceProperty queueProperty, String... files) {
        sentJobs.addJobId(ingest.sendJobGetId(queueProperty, sourceFiles(files)));
        return this;
    }

    public IngestByQueueDsl sendSourceFilesToAllTables(String... files) {
        sentJobs.addAllJobIds(ingest.sendJobToAllTablesGetIds(defaultQueueProperty, sourceFiles(files)));
        return this;
    }

    public IngestByQueueDsl waitForTask() {
        tasksDriver.waitForTasksForCurrentInstance().waitUntilOneTaskStartedAJob(sentJobs.getJobIds(), pollDriver);
        return this;
    }

    public void waitForJobs() {
        waitForJobs.waitForJobs(sentJobs.getJobIds());
    }

    public void waitForJobs(PollWithRetries pollWithRetries) {
        waitForJobs.waitForJobs(sentJobs.getJobIds(), pollWithRetries);
    }

    private List<String> sourceFiles(String... files) {
        return sourceFiles.lastFolderWrittenTo().getIngestJobFilesInBucket(Stream.of(files));
    }
}
