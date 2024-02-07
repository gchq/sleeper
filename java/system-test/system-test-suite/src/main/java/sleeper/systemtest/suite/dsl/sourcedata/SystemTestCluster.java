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

package sleeper.systemtest.suite.dsl.sourcedata;

import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;
import sleeper.systemtest.drivers.ingest.AwsDataGenerationTasksDriver;
import sleeper.systemtest.drivers.ingest.IngestByQueueDriver;
import sleeper.systemtest.drivers.sourcedata.GeneratedIngestSourceFiles;
import sleeper.systemtest.drivers.sourcedata.GeneratedIngestSourceFilesDriver;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.drivers.util.WaitForJobsDriver;
import sleeper.systemtest.dsl.instance.SleeperInstanceContext;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentContext;
import sleeper.systemtest.dsl.sourcedata.DataGenerationTasksDriver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class SystemTestCluster {

    private final SystemTestDeploymentContext context;
    private final DataGenerationTasksDriver driver;
    private final IngestByQueueDriver byQueueDriver;
    private final GeneratedIngestSourceFilesDriver sourceFiles;
    private final WaitForJobsDriver waitForIngestJobsDriver;
    private final WaitForJobsDriver waitForBulkImportJobsDriver;
    private GeneratedIngestSourceFiles lastGeneratedFiles;
    private final List<String> jobIds = new ArrayList<>();

    public SystemTestCluster(SystemTestClients clients,
                             SystemTestDeploymentContext context,
                             SleeperInstanceContext instance) {
        this.context = context;
        this.driver = new AwsDataGenerationTasksDriver(context, instance, clients.getEcs());
        this.byQueueDriver = new IngestByQueueDriver(instance, clients.getDynamoDB(), clients.getLambda(), clients.getSqs());
        this.sourceFiles = new GeneratedIngestSourceFilesDriver(context, clients.getS3V2());
        this.waitForIngestJobsDriver = WaitForJobsDriver.forIngest(instance, clients.getDynamoDB());
        this.waitForBulkImportJobsDriver = WaitForJobsDriver.forBulkImport(instance, clients.getDynamoDB());
    }

    public SystemTestCluster updateProperties(Consumer<SystemTestStandaloneProperties> config) {
        context.updateProperties(config);
        return this;
    }

    public SystemTestCluster generateData() {
        return generateData(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(2)));
    }

    public SystemTestCluster generateData(PollWithRetries poll) {
        driver.runDataGenerationTasks(poll);
        lastGeneratedFiles = sourceFiles.findGeneratedFiles();
        return this;
    }

    public SystemTestCluster sendAllGeneratedFilesAsOneJob(InstanceProperty queueUrlProperty) {
        jobIds.add(byQueueDriver.sendJobGetId(queueUrlProperty, lastGeneratedFiles.getIngestJobFilesCombiningAll()));
        return this;
    }

    public SystemTestCluster invokeStandardIngestTask() throws InterruptedException {
        byQueueDriver.invokeStandardIngestTask();
        return this;
    }

    public SystemTestCluster invokeStandardIngestTasks(int expectedTasks, PollWithRetries poll) throws InterruptedException {
        byQueueDriver.invokeStandardIngestTasks(expectedTasks, poll);
        return this;
    }

    public void waitForIngestJobs() throws InterruptedException {
        waitForIngestJobsDriver.waitForJobs(jobIds());
    }

    public void waitForIngestJobs(PollWithRetries poll) throws InterruptedException {
        waitForIngestJobsDriver.waitForJobs(jobIds(), poll);
    }

    public void waitForBulkImportJobs(PollWithRetries poll) throws InterruptedException {
        waitForBulkImportJobsDriver.waitForJobs(jobIds(), poll);
    }

    private List<String> jobIds() {
        if (jobIds.isEmpty()) {
            jobIds.addAll(lastGeneratedFiles.getJobIdsFromIndividualFiles());
        }
        return jobIds;
    }

    public List<String> findIngestJobIdsInSourceBucket() {
        return sourceFiles.findGeneratedFiles().getJobIdsFromIndividualFiles();
    }

    public boolean isDisabled() {
        return !context.isSystemTestClusterEnabled();
    }
}
