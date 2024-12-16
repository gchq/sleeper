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

package sleeper.systemtest.dsl.sourcedata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.statestore.FileReference;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.ingest.IngestByQueue;
import sleeper.systemtest.dsl.ingest.IngestTasksDriver;
import sleeper.systemtest.dsl.instance.DeployedSystemTestResources;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class SystemTestCluster {
    public static final Logger LOGGER = LoggerFactory.getLogger(SystemTestCluster.class);

    private final DeployedSystemTestResources context;
    private final SystemTestInstanceContext instance;
    private final DataGenerationTasksDriver driver;
    private final IngestByQueue ingestByQueue;
    private final GeneratedIngestSourceFilesDriver sourceFiles;
    private final IngestTasksDriver tasksDriver;
    private final WaitForJobs waitForIngestJobs;
    private final WaitForJobs waitForBulkImportJobs;
    private final PollWithRetriesDriver pollDriver;
    private GeneratedIngestSourceFiles lastGeneratedFiles = null;
    private final List<String> jobIds = new ArrayList<>();

    public SystemTestCluster(
            SystemTestContext context, SystemTestDrivers baseDrivers) {
        this.context = context.systemTest();
        instance = context.instance();
        SystemTestDrivers instanceAdminDrivers = instance.adminDrivers();
        driver = baseDrivers.dataGenerationTasks(context);
        ingestByQueue = instanceAdminDrivers.ingestByQueue(context);
        sourceFiles = baseDrivers.generatedSourceFiles(context.parameters(), context.systemTest());
        tasksDriver = instanceAdminDrivers.ingestTasks(context);
        waitForIngestJobs = instanceAdminDrivers.waitForIngest(context);
        waitForBulkImportJobs = instanceAdminDrivers.waitForBulkImport(context);
        pollDriver = instanceAdminDrivers.pollWithRetries();
    }

    public SystemTestCluster updateProperties(Consumer<SystemTestStandaloneProperties> config) {
        context.updateProperties(config);
        return this;
    }

    public SystemTestCluster runDataGenerationTasks() {
        return runDataGenerationTasks(pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(10), Duration.ofMinutes(2)));
    }

    public SystemTestCluster runDataGenerationTasks(PollWithRetries poll) {
        driver.runDataGenerationTasks(poll);
        lastGeneratedFiles = sourceFiles.findGeneratedFiles();
        return this;
    }

    public SystemTestCluster sendAllGeneratedFilesAsOneJob(InstanceProperty queueUrlProperty) {
        jobIds.add(ingestByQueue.sendJobGetId(queueUrlProperty, lastGeneratedFiles.getIngestJobFilesCombiningAll()));
        return this;
    }

    public SystemTestCluster invokeStandardIngestTask() {
        tasksDriver.waitForTasksForCurrentInstance().waitUntilOneTaskStartedAJob(jobIds(), pollDriver);
        return this;
    }

    public SystemTestCluster invokeStandardIngestTasks(int expectedTasks, PollWithRetries poll) {
        tasksDriver.waitForTasksForCurrentInstance().waitUntilNumTasksStartedAJob(expectedTasks, jobIds(), poll);
        return this;
    }

    public void waitForIngestJobs() {
        waitForIngestJobs.waitForJobs(jobIds());
    }

    public void waitForIngestJobs(PollWithRetries poll) {
        waitForIngestJobs.waitForJobs(jobIds(), poll);
    }

    public void waitForBulkImportJobs(PollWithRetries poll) {
        waitForBulkImportJobs.waitForJobs(jobIds(), poll);
    }

    private List<String> jobIds() {
        if (jobIds.isEmpty()) {
            jobIds.addAll(lastGeneratedFiles.getJobIdsFromIndividualFiles());
        }
        return jobIds;
    }

    public void waitForTotalFileReferences(int expectedFileReferences) {
        waitForTotalFileReferences(expectedFileReferences,
                pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(5), Duration.ofMinutes(1)));
    }

    public void waitForTotalFileReferences(int expectedFileReferences, PollWithRetries poll) {
        try {
            poll.pollUntil("file references are added", () -> {
                List<FileReference> fileReferences = loadFileReferences();
                LOGGER.info("Found {} file references, waiting for expected {}", fileReferences.size(), expectedFileReferences);
                if (fileReferences.size() > expectedFileReferences) {
                    throw new RuntimeException("Was waiting for " + expectedFileReferences + " file references, overshot and found " + fileReferences.size());
                }
                return fileReferences.size() == expectedFileReferences;
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private List<FileReference> loadFileReferences() {
        return instance.getStateStore().getFileReferences();
    }

    public List<String> findIngestJobIdsInSourceBucket() {
        return sourceFiles.findGeneratedFiles().getJobIdsFromIndividualFiles();
    }

    public boolean isDisabled() {
        return !context.isSystemTestClusterEnabled();
    }
}
