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

package sleeper.systemtest.suite.dsl.python;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.drivers.ingest.IngestByQueueDriver;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.python.PythonIngestDriver;
import sleeper.systemtest.drivers.util.WaitForJobsDriver;
import sleeper.systemtest.suite.fixtures.SystemTestClients;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SystemTestPythonIngest {
    private final PythonIngestDriver pythonIngestDriver;
    private final IngestByQueueDriver ingestByQueueDriver;
    private final WaitForJobsDriver waitForJobsDriver;
    private final List<String> sentJobIds = new ArrayList<>();


    public SystemTestPythonIngest(SleeperInstanceContext instance, SystemTestClients clients,
                                  Path pythonDir) {
        this.pythonIngestDriver = new PythonIngestDriver(instance, pythonDir);
        this.ingestByQueueDriver = new IngestByQueueDriver(instance,
                clients.getDynamoDB(), clients.getLambda(), clients.getSqs());
        this.waitForJobsDriver = WaitForJobsDriver.forIngest(instance, clients.getDynamoDB());
    }

    public SystemTestPythonIngest uploadingLocalFile(Path tempDir, String file) throws IOException, InterruptedException {
        String jobId = UUID.randomUUID().toString();
        pythonIngestDriver.batchWrite(tempDir, jobId, file);
        sentJobIds.add(jobId);
        return this;
    }

    public SystemTestPythonIngest fromS3(String... files) throws IOException, InterruptedException {
        String jobId = UUID.randomUUID().toString();
        pythonIngestDriver.fromS3(jobId, files);
        sentJobIds.add(jobId);
        return this;
    }

    public SystemTestPythonIngest invokeTask() throws InterruptedException {
        ingestByQueueDriver.invokeStandardIngestTask();
        return this;
    }

    public void waitForJobs() throws InterruptedException {
        waitForJobsDriver.waitForJobs(sentJobIds,
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(10)));
    }
}
