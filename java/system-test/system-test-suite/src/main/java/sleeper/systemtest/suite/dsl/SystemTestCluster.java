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

import com.amazonaws.services.ecs.model.Task;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.configuration.IngestMode;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;
import sleeper.systemtest.drivers.ingest.DataGenerationDriver;
import sleeper.systemtest.drivers.ingest.IngestByQueueDriver;
import sleeper.systemtest.drivers.ingest.IngestSourceFilesDriver;
import sleeper.systemtest.drivers.ingest.WaitForIngestJobsDriver;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.instance.SystemTestInstanceContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_WRITER;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_WRITERS;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_CLUSTER_ENABLED;

public class SystemTestCluster {

    private final SystemTestInstanceContext context;
    private final DataGenerationDriver driver;
    private final IngestByQueueDriver byQueueDriver;
    private final IngestSourceFilesDriver sourceFiles;
    private final WaitForIngestJobsDriver waitForJobsDriver;
    private final List<String> jobIds = new ArrayList<>();

    public SystemTestCluster(SystemTestInstanceContext context, SleeperInstanceContext instance, SystemTestClients clients) {
        this.context = context;
        this.driver = new DataGenerationDriver(context, instance, clients.getEcs());
        this.byQueueDriver = new IngestByQueueDriver(instance, clients.getDynamoDB(), clients.getLambda(), clients.getSqs());
        this.sourceFiles = new IngestSourceFilesDriver(context, clients.getS3V2());
        this.waitForJobsDriver = new WaitForIngestJobsDriver(instance, clients.getDynamoDB());
    }

    public void ingestDirectRecords(int records) throws InterruptedException {
        updateProperties(properties -> {
            properties.set(INGEST_MODE, IngestMode.DIRECT.toString());
            properties.set(NUMBER_OF_WRITERS, "1");
            properties.set(NUMBER_OF_RECORDS_PER_WRITER, String.valueOf(records));
        }).generateData(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(2)));
    }

    public SystemTestCluster updateProperties(Consumer<SystemTestStandaloneProperties> config) {
        context.updateProperties(config);
        return this;
    }

    public SystemTestCluster generateData(PollWithRetries poll) throws InterruptedException {
        List<Task> tasks = driver.startTasks();
        driver.waitForTasks(tasks, poll);
        jobIds.addAll(sourceFiles.findGeneratedIngestJobIds());
        return this;
    }

    public SystemTestCluster invokeStandardIngestTasks(int expectedTasks, PollWithRetries poll) throws InterruptedException {
        byQueueDriver.invokeStandardIngestTasks(expectedTasks, poll);
        return this;
    }

    public void waitForJobs(PollWithRetries poll) throws InterruptedException {
        waitForJobsDriver.waitForJobs(jobIds, poll);
    }

    public boolean isDisabled() {
        return !context.getProperties().getBoolean(SYSTEM_TEST_CLUSTER_ENABLED);
    }
}
