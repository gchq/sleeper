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

package sleeper.systemtest.dsl.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.function.Function;

public class WaitForJobs {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForJobs.class);

    private final SystemTestInstanceContext instance;
    private final String typeDescription;
    private final Function<InstanceProperties, JobStatusStore> getJobsStore;
    private final Function<InstanceProperties, TaskStatusStore> getTasksStore;

    private WaitForJobs(
            SystemTestInstanceContext instance, String typeDescription,
            Function<InstanceProperties, JobStatusStore> getJobsStore,
            Function<InstanceProperties, TaskStatusStore> getTasksStore) {
        this.instance = instance;
        this.typeDescription = typeDescription;
        this.getJobsStore = getJobsStore;
        this.getTasksStore = getTasksStore;
    }

    public static WaitForJobs forIngest(
            SystemTestInstanceContext instance,
            Function<InstanceProperties, IngestJobStatusStore> getJobsStore,
            Function<InstanceProperties, IngestTaskStatusStore> getTasksStore) {
        return new WaitForJobs(instance, "ingest",
                properties -> JobStatusStore.forIngest(getJobsStore.apply(properties)),
                properties -> TaskStatusStore.forIngest(getTasksStore.apply(properties)));
    }

    public static WaitForJobs forBulkImport(
            SystemTestInstanceContext instance,
            Function<InstanceProperties, IngestJobStatusStore> getJobsStore) {
        return new WaitForJobs(instance, "bulk import",
                properties -> JobStatusStore.forIngest(getJobsStore.apply(properties)),
                properties -> () -> true);
    }

    public static WaitForJobs forCompaction(
            SystemTestInstanceContext instance,
            Function<InstanceProperties, CompactionJobStatusStore> getJobsStore,
            Function<InstanceProperties, CompactionTaskStatusStore> getTasksStore) {
        return new WaitForJobs(instance, "compaction",
                properties -> JobStatusStore.forCompaction(getJobsStore.apply(properties)),
                properties -> TaskStatusStore.forCompaction(getTasksStore.apply(properties)));
    }

    public void waitForJobs(Collection<String> jobIds) {
        waitForJobs(jobIds, PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(1), Duration.ofMinutes(10)));
    }

    public void waitForJobs(Collection<String> jobIds, PollWithRetries pollUntilJobsFinished) {
        waitForJobs(jobIds, pollUntilJobsFinished,
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(1), Duration.ofSeconds(30)));
    }

    public void waitForJobs(
            Collection<String> jobIds, PollWithRetries pollUntilJobsFinished, PollWithRetries pollUntilJobsCommit) {
        InstanceProperties properties = instance.getInstanceProperties();
        JobStatusStore store = getJobsStore.apply(properties);
        TaskStatusStore tasksStore = getTasksStore.apply(properties);
        LOGGER.info("Waiting for {} jobs to finish: {}", typeDescription, jobIds.size());
        try {
            pollUntilJobsFinished.pollUntil("jobs are finished", () -> {
                WaitForJobsStatus status = store.getStatus(jobIds);
                LOGGER.info("Status of {} jobs: {}", typeDescription, status);
                if (status.areAllJobsFinished()) {
                    return true;
                }
                if (tasksStore.hasRunningTasks()) {
                    return false;
                } else {
                    LOGGER.info("Found no running tasks while waiting for {} jobs, will wait for async commits", typeDescription);
                    waitForJobsToCommit(jobIds, store, pollUntilJobsCommit);
                    return true;
                }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void waitForJobsToCommit(
            Collection<String> jobIds, JobStatusStore store, PollWithRetries pollUntilJobsCommit) {
        try {
            pollUntilJobsCommit.pollUntil("jobs are committed", () -> {
                WaitForJobsStatus status = store.getStatus(jobIds);
                LOGGER.info("Status of {} jobs waiting for async commits: {}", typeDescription, status);
                return status.areAllJobsFinished();
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    private interface JobStatusStore {
        WaitForJobsStatus getStatus(Collection<String> jobIds);

        static JobStatusStore forIngest(IngestJobStatusStore store) {
            return jobId -> WaitForJobsStatus.forIngest(store, jobId, Instant.now());
        }

        static JobStatusStore forCompaction(CompactionJobStatusStore store) {
            return jobId -> WaitForJobsStatus.forCompaction(store, jobId, Instant.now());
        }
    }

    @FunctionalInterface
    private interface TaskStatusStore {
        boolean hasRunningTasks();

        static TaskStatusStore forIngest(IngestTaskStatusStore store) {
            return () -> !store.getTasksInProgress().isEmpty();
        }

        static TaskStatusStore forCompaction(CompactionTaskStatusStore store) {
            return () -> !store.getTasksInProgress().isEmpty();
        }
    }
}
