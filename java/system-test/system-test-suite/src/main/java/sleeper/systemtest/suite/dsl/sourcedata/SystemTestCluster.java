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

package sleeper.systemtest.suite.dsl.sourcedata;

import com.amazonaws.services.ecs.model.Task;

import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;
import sleeper.systemtest.drivers.ingest.DataGenerationDriver;
import sleeper.systemtest.drivers.ingest.GeneratedIngestSourceFiles;
import sleeper.systemtest.drivers.ingest.IngestByQueueDriver;
import sleeper.systemtest.drivers.ingest.IngestSourceFilesDriver;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.instance.SystemTestDeploymentContext;
import sleeper.systemtest.drivers.util.WaitForJobsDriver;
import sleeper.systemtest.suite.fixtures.SystemTestClients;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_CLUSTER_ENABLED;

public class SystemTestCluster {

    private final SystemTestDeploymentContext context;
    private final DataGenerationDriver driver;
    private final IngestByQueueDriver byQueueDriver;
    private final IngestSourceFilesDriver sourceFiles;
    private final WaitForJobsDriver waitForJobsDriver;
    private GeneratedIngestSourceFiles lastGeneratedFiles;
    private final List<String> jobIds = new ArrayList<>();

    public SystemTestCluster(SystemTestDeploymentContext context, SleeperInstanceContext instance, SystemTestClients clients) {
        this.context = context;
        this.driver = new DataGenerationDriver(context, instance, clients.getEcs());
        this.byQueueDriver = new IngestByQueueDriver(instance, clients.getDynamoDB(), clients.getLambda(), clients.getSqs());
        this.sourceFiles = new IngestSourceFilesDriver(context, clients.getS3V2());
        this.waitForJobsDriver = WaitForJobsDriver.forIngest(instance, clients.getDynamoDB());
    }

    public SystemTestCluster updateProperties(Consumer<SystemTestStandaloneProperties> config) {
        context.updateProperties(config);
        return this;
    }

    public SystemTestCluster generateData() throws InterruptedException {
        return generateData(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(2)));
    }

    public SystemTestCluster generateData(PollWithRetries poll) throws InterruptedException {
        List<Task> tasks = driver.startTasks();
        driver.waitForTasks(tasks, poll);
        lastGeneratedFiles = sourceFiles.findGeneratedFiles();
        return this;
    }

    public SystemTestCluster sendAllGeneratedFilesAsOneJob(InstanceProperty queueProperty) {
        String jobId = UUID.randomUUID().toString();
        byQueueDriver.sendJob(queueProperty, jobId, lastGeneratedFiles.getIngestJobFilesCombiningAll());
        jobIds.add(jobId);
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

    public void waitForJobs() throws InterruptedException {
        waitForJobsDriver.waitForJobs(jobIds());
    }

    public void waitForJobs(PollWithRetries poll) throws InterruptedException {
        waitForJobsDriver.waitForJobs(jobIds(), poll);
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
        return !context.getProperties().getBoolean(SYSTEM_TEST_CLUSTER_ENABLED);
    }
}
