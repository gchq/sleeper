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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.job.status.IngestJobStatusStore;

import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class WaitForIngestTasks {
    public static final Logger LOGGER = LoggerFactory.getLogger(WaitForIngestTasks.class);

    private final InvokeIngestTasksDriverNew invokeDriver;
    private final IngestJobStatusStore jobStatusStore;

    public WaitForIngestTasks(InvokeIngestTasksDriverNew invokeDriver, IngestJobStatusStore jobStatusStore) {
        this.invokeDriver = invokeDriver;
        this.jobStatusStore = jobStatusStore;
    }

    public void invokeUntilNumTasksStartedAJob(int expectedTasks, List<String> jobIds, PollWithRetries poll) throws InterruptedException {
        if (numTasksStartedAJob(jobIds) >= expectedTasks) {
            return;
        }
        poll.pollUntil("expected number of tasks running given jobs", () -> {
            invokeDriver.invokeStandardIngestTaskCreator();
            return numTasksStartedAJob(jobIds) >= expectedTasks;
        });
    }

    private int numTasksStartedAJob(List<String> jobIds) {
        Set<String> taskIds = jobIds.stream()
                .flatMap(jobId -> jobStatusStore.getJob(jobId).stream())
                .flatMap(status -> status.getJobRuns().stream())
                .map(ProcessRun::getTaskId)
                .collect(toSet());
        LOGGER.info("Found {} tasks with runs for given jobs", taskIds.size());
        return taskIds.size();
    }
}
