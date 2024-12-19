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

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.core.job.status.IngestJobStatusStore;
import sleeper.ingest.core.task.IngestTaskStatusStore;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.function.Function;

public class WaitForJobs {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForJobs.class);

    private final SystemTestInstanceContext instance;
    private final String typeDescription;
    private final Function<InstanceProperties, JobTracker> getJobTracker;
    private final Function<InstanceProperties, TaskTracker> getTaskTracker;
    private final PollWithRetriesDriver pollDriver;

    private WaitForJobs(
            SystemTestInstanceContext instance, String typeDescription,
            Function<InstanceProperties, JobTracker> getJobTracker,
            Function<InstanceProperties, TaskTracker> getTaskTracker,
            PollWithRetriesDriver pollDriver) {
        this.instance = instance;
        this.typeDescription = typeDescription;
        this.getJobTracker = getJobTracker;
        this.getTaskTracker = getTaskTracker;
        this.pollDriver = pollDriver;
    }

    public static WaitForJobs forIngest(
            SystemTestInstanceContext instance,
            Function<InstanceProperties, IngestJobStatusStore> getJobTracker,
            Function<InstanceProperties, IngestTaskStatusStore> getTaskTracker,
            PollWithRetriesDriver pollDriver) {
        return new WaitForJobs(instance, "ingest",
                properties -> JobTracker.forIngest(getJobTracker.apply(properties)),
                properties -> TaskTracker.forIngest(getTaskTracker.apply(properties)),
                pollDriver);
    }

    public static WaitForJobs forBulkImport(
            SystemTestInstanceContext instance,
            Function<InstanceProperties, IngestJobStatusStore> getJobTracker,
            PollWithRetriesDriver pollDriver) {
        return new WaitForJobs(instance, "bulk import",
                properties -> JobTracker.forIngest(getJobTracker.apply(properties)),
                properties -> () -> true,
                pollDriver);
    }

    public static WaitForJobs forCompaction(
            SystemTestInstanceContext instance,
            Function<InstanceProperties, CompactionJobTracker> getJobTracker,
            Function<InstanceProperties, CompactionTaskTracker> getTaskTracker,
            PollWithRetriesDriver pollDriver) {
        return new WaitForJobs(instance, "compaction",
                properties -> JobTracker.forCompaction(getJobTracker.apply(properties)),
                properties -> TaskTracker.forCompaction(getTaskTracker.apply(properties)),
                pollDriver);
    }

    public void waitForJobs(Collection<String> jobIds) {
        waitForJobs(jobIds, pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(1), Duration.ofMinutes(10)));
    }

    public void waitForJobs(Collection<String> jobIds, PollWithRetries pollUntilJobsFinished) {
        waitForJobs(jobIds, pollUntilJobsFinished,
                pollDriver.pollWithIntervalAndTimeout(Duration.ofSeconds(1), Duration.ofSeconds(30)));
    }

    public void waitForJobs(
            Collection<String> jobIds, PollWithRetries pollUntilJobsFinished, PollWithRetries pollUntilJobsCommit) {
        InstanceProperties properties = instance.getInstanceProperties();
        JobTracker jobTracker = getJobTracker.apply(properties);
        TaskTracker taskTracker = getTaskTracker.apply(properties);
        LOGGER.info("Waiting for {} jobs to finish: {}", typeDescription, jobIds.size());
        try {
            pollUntilJobsFinished.pollUntil("jobs are finished", () -> {
                WaitForJobsStatus status = jobTracker.getStatus(jobIds);
                LOGGER.info("Status of {} jobs: {}", typeDescription, status);
                if (status.areAllJobsFinished()) {
                    return true;
                }
                if (taskTracker.hasRunningTasks()) {
                    return false;
                } else {
                    LOGGER.info("Found no running tasks while waiting for {} jobs, will wait for async commits", typeDescription);
                    waitForJobsToCommit(jobIds, jobTracker, pollUntilJobsCommit);
                    return true;
                }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void waitForJobsToCommit(
            Collection<String> jobIds, JobTracker tracker, PollWithRetries pollUntilJobsCommit) {
        try {
            pollUntilJobsCommit.pollUntil("jobs are committed", () -> {
                WaitForJobsStatus status = tracker.getStatus(jobIds);
                LOGGER.info("Status of {} jobs waiting for async commits: {}", typeDescription, status);
                return status.areAllJobsFinished();
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    private interface JobTracker {
        WaitForJobsStatus getStatus(Collection<String> jobIds);

        static JobTracker forIngest(IngestJobStatusStore tracker) {
            return jobId -> WaitForJobsStatus.forIngest(tracker, jobId, Instant.now());
        }

        static JobTracker forCompaction(CompactionJobTracker tracker) {
            return jobId -> WaitForJobsStatus.forCompaction(tracker, jobId, Instant.now());
        }
    }

    @FunctionalInterface
    private interface TaskTracker {
        boolean hasRunningTasks();

        static TaskTracker forIngest(IngestTaskStatusStore tracker) {
            return () -> !tracker.getTasksInProgress().isEmpty();
        }

        static TaskTracker forCompaction(CompactionTaskTracker tracker) {
            return () -> !tracker.getTasksInProgress().isEmpty();
        }
    }
}
