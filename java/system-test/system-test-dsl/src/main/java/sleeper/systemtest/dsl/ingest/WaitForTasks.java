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

import sleeper.compaction.core.job.CompactionJobStatusStore;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.core.job.status.IngestJobStatusStore;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableSet;

public class WaitForTasks {
    public static final Logger LOGGER = LoggerFactory.getLogger(WaitForTasks.class);

    private final JobStatusStore jobStatusStore;

    public WaitForTasks(IngestJobStatusStore jobStatusStore) {
        this.jobStatusStore = JobStatusStore.forIngest(jobStatusStore);
    }

    public WaitForTasks(CompactionJobStatusStore jobStatusStore) {
        this.jobStatusStore = JobStatusStore.forCompaction(jobStatusStore);
    }

    public void waitUntilOneTaskStartedAJob(List<String> jobIds, PollWithRetriesDriver pollDriver) {
        waitUntilNumTasksStartedAJob(1, jobIds,
                pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(10), Duration.ofMinutes(3)));
    }

    public void waitUntilNumTasksStartedAJob(int expectedTasks, List<String> jobIds, PollWithRetries poll) {
        if (jobIds.isEmpty()) {
            throw new IllegalArgumentException("Need jobs to wait for before invoking tasks, none are yet specified");
        }
        if (numTasksStartedAJob(jobIds) >= expectedTasks) {
            return;
        }
        try {
            poll.pollUntil("expected number of tasks have picked up a job", () -> {
                return numTasksStartedAJob(jobIds) >= expectedTasks;
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private int numTasksStartedAJob(List<String> jobIds) {
        Set<String> taskIds = jobStatusStore.findRunsOfJobs(jobIds)
                .map(ProcessRun::getTaskId)
                .collect(toUnmodifiableSet());
        LOGGER.info("Found {} tasks with runs for given jobs", taskIds.size());
        return taskIds.size();
    }

    @FunctionalInterface
    private interface JobStatusStore {
        Stream<ProcessRun> findRunsOfJobs(Collection<String> jobIds);

        static JobStatusStore forIngest(IngestJobStatusStore store) {
            return jobIds -> jobIds.stream().parallel()
                    .flatMap(jobId -> store.getJob(jobId).stream())
                    .flatMap(job -> job.getJobRuns().stream());
        }

        static JobStatusStore forCompaction(CompactionJobStatusStore store) {
            return jobIds -> jobIds.stream().parallel()
                    .flatMap(jobId -> store.getJob(jobId).stream())
                    .flatMap(job -> job.getJobRuns().stream());
        }
    }
}
