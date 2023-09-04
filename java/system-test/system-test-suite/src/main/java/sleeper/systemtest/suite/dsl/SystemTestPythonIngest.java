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

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.drivers.ingest.IngestByQueueDriver;
import sleeper.systemtest.drivers.ingest.WaitForIngestJobsDriver;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.python.PythonIngestDriver;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class SystemTestPythonIngest {
    private final PythonIngestDriver pythonIngestDriver;
    private final IngestByQueueDriver ingestByQueueDriver;
    private final WaitForIngestJobsDriver waitForJobsDriver;
    private final List<String> sentJobIds = new ArrayList<>();


    public SystemTestPythonIngest(SleeperInstanceContext instance, SystemTestClients clients,
                                  Path pythonDir, Path tempDir) {
        this.pythonIngestDriver = new PythonIngestDriver(instance, pythonDir, tempDir);
        this.ingestByQueueDriver = new IngestByQueueDriver(instance,
                clients.getDynamoDB(), clients.getLambda(), clients.getSqs());
        this.waitForJobsDriver = new WaitForIngestJobsDriver(instance, clients.getDynamoDB());
    }

    public SystemTestPythonIngest batchWrite(String jobId, String file) throws IOException, InterruptedException {
        pythonIngestDriver.batchWrite(jobId, file);
        sentJobIds.add(jobId);
        return this;
    }

    public SystemTestPythonIngest fromS3(String jobId, String... files) throws IOException, InterruptedException {
        pythonIngestDriver.fromS3(jobId, files);
        sentJobIds.add(jobId);
        return this;
    }

    public SystemTestPythonIngest invokeTasks() throws InterruptedException {
        ingestByQueueDriver.invokeStandardIngestTasks();
        return this;
    }

    public void waitForJobs() throws InterruptedException {
        waitForJobsDriver.waitForJobs(sentJobIds,
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(10)));
    }
}
