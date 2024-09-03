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

package sleeper.systemtest.dsl.python;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.ingest.IngestByAnyQueueDriver;
import sleeper.systemtest.dsl.ingest.IngestLocalFileByAnyQueueDriver;
import sleeper.systemtest.dsl.ingest.InvokeIngestTasksDriverNew;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SystemTestPythonIngest {
    private final IngestByAnyQueueDriver fromS3Driver;
    private final IngestLocalFileByAnyQueueDriver localFileDriver;
    private final InvokeIngestTasksDriverNew tasksDriver;
    private final WaitForJobs waitForJobs;
    private final List<String> sentJobIds = new ArrayList<>();

    public SystemTestPythonIngest(
            IngestByAnyQueueDriver fromS3Driver, IngestLocalFileByAnyQueueDriver localFileDriver,
            InvokeIngestTasksDriverNew tasksDriver, WaitForJobs waitForJobs) {
        this.fromS3Driver = fromS3Driver;
        this.localFileDriver = localFileDriver;
        this.tasksDriver = tasksDriver;
        this.waitForJobs = waitForJobs;
    }

    public SystemTestPythonIngest uploadingLocalFile(Path tempDir, String file) {
        String jobId = UUID.randomUUID().toString();
        localFileDriver.uploadLocalFileAndSendJob(tempDir, jobId, file);
        sentJobIds.add(jobId);
        return this;
    }

    public SystemTestPythonIngest fromS3(String... files) {
        String jobId = UUID.randomUUID().toString();
        fromS3Driver.sendJobWithFiles(jobId, files);
        sentJobIds.add(jobId);
        return this;
    }

    public SystemTestPythonIngest invokeTask() {
        tasksDriver.invokeTasksForCurrentInstance().invokeUntilOneTaskStartedAJob(sentJobIds);
        return this;
    }

    public void waitForJobs() {
        waitForJobs.waitForJobs(sentJobIds,
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(10)));
    }
}
