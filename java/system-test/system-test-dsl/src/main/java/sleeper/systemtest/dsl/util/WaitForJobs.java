/*
 * Copyright 2022-2025 Crown Copyright
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
import sleeper.core.properties.table.TableProperties;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.task.IngestTaskTracker;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class WaitForJobs {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForJobs.class);

    private final Supplier<InstanceProperties> getInstanceProperties;
    private final String typeDescription;
    private final Function<InstanceProperties, JobTracker> getJobTracker;
    private final Function<InstanceProperties, TaskTracker> getTaskTracker;
    private final PollWithRetriesDriver pollDriver;

    WaitForJobs(
            Supplier<InstanceProperties> getInstanceProperties, String typeDescription,
            Function<InstanceProperties, JobTracker> getJobTracker,
            Function<InstanceProperties, TaskTracker> getTaskTracker,
            PollWithRetriesDriver pollDriver) {
        this.getInstanceProperties = getInstanceProperties;
        this.typeDescription = typeDescription;
        this.getJobTracker = getJobTracker;
        this.getTaskTracker = getTaskTracker;
        this.pollDriver = pollDriver;
    }

    public static WaitForJobs forIngest(
            SystemTestInstanceContext instance,
            Function<InstanceProperties, IngestJobTracker> getJobTracker,
            Function<InstanceProperties, IngestTaskTracker> getTaskTracker,
            PollWithRetriesDriver pollDriver) {
        return new WaitForJobs(instance::getInstanceProperties, "ingest",
                properties -> JobTracker.forIngest(instance.currentTablePropertiesCollection(), getJobTracker.apply(properties)),
                properties -> TaskTracker.forIngest(getTaskTracker.apply(properties)),
                pollDriver);
    }

    public static WaitForJobs forBulkImport(
            SystemTestInstanceContext instance,
            Function<InstanceProperties, IngestJobTracker> getJobTracker,
            PollWithRetriesDriver pollDriver) {
        return new WaitForJobs(instance::getInstanceProperties, "bulk import",
                properties -> JobTracker.forIngest(instance.currentTablePropertiesCollection(), getJobTracker.apply(properties)),
                properties -> () -> true,
                pollDriver);
    }

    public static WaitForJobs forCompaction(
            SystemTestInstanceContext instance,
            Function<InstanceProperties, CompactionJobTracker> getJobTracker,
            Function<InstanceProperties, CompactionTaskTracker> getTaskTracker,
            PollWithRetriesDriver pollDriver) {
        return new WaitForJobs(instance::getInstanceProperties, "compaction",
                properties -> JobTracker.forCompaction(instance.currentTablePropertiesCollection(), getJobTracker.apply(properties)),
                properties -> TaskTracker.forCompaction(getTaskTracker.apply(properties)),
                pollDriver);
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
        InstanceProperties properties = getInstanceProperties.get();
        JobTracker jobTracker = getJobTracker.apply(properties);
        TaskTracker taskTracker = getTaskTracker.apply(properties);
        LOGGER.info("Waiting for {} jobs to finish: {}", typeDescription, jobIds.size());
        try {
            pollDriver.poll(pollUntilJobsFinished).pollUntil("jobs are finished", () -> {
                WaitForJobsStatus status = jobTracker.getStatus(jobIds);
                LOGGER.info("Status of {} jobs: {}", typeDescription, status);
                if (status.areAllJobsFinished()) {
                    return true;
                }
                if (taskTracker.hasRunningTasks()) {
                    return false;
                } else {
                    LOGGER.info("Found no running tasks while waiting for {} jobs, will wait for async commits", typeDescription);
                    waitForJobsToCommit(() -> jobTracker.getStatus(jobIds), pollUntilJobsCommit);
                    return true;
                }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public void waitForJobsToCommit(
            Collection<String> jobIds, PollWithRetries pollUntilJobsCommit) {
        InstanceProperties properties = getInstanceProperties.get();
        JobTracker jobTracker = getJobTracker.apply(properties);
        LOGGER.info("Waiting for {} jobs to commit: {}", typeDescription, jobIds.size());
        waitForJobsToCommit(() -> jobTracker.getStatus(jobIds), pollUntilJobsCommit);
    }

    public void waitForAllJobsToCommit(PollWithRetries pollUntilJobsCommit) {
        InstanceProperties properties = getInstanceProperties.get();
        JobTracker jobTracker = getJobTracker.apply(properties);
        LOGGER.info("Waiting for all {} jobs to commit", typeDescription);
        waitForJobsToCommit(() -> jobTracker.getAllJobsStatus(), pollUntilJobsCommit);
    }

    private void waitForJobsToCommit(Supplier<WaitForJobsStatus> getStatus, PollWithRetries pollUntilJobsCommit) {
        try {
            pollDriver.poll(pollUntilJobsCommit).pollUntil("jobs are committed", () -> {
                WaitForJobsStatus status = getStatus.get();
                LOGGER.info("Status of {} jobs waiting for async commits: {}", typeDescription, status);
                return status.areAllJobsFinished();
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    public interface JobTracker {
        Stream<WaitForJobsStatus.JobStatus<?>> streamAllJobsInTest();

        default WaitForJobsStatus getAllJobsStatus() {
            return getAllJobsStatus(Instant.now());
        }

        default WaitForJobsStatus getAllJobsStatus(Instant now) {
            return WaitForJobsStatus.atTime(now).report(streamAllJobsInTest()).build();
        }

        default WaitForJobsStatus getStatus(Collection<String> jobIds) {
            return getStatus(jobIds, Instant.now());
        }

        default WaitForJobsStatus getStatus(Collection<String> jobIds, Instant now) {
            return WaitForJobsStatus.atTime(now).reportById(jobIds, streamAllJobsInTest()).build();
        }

        static JobTracker forIngest(Collection<TableProperties> tables, IngestJobTracker tracker) {
            return () -> WaitForJobsStatus.streamIngestJobs(tracker, tables);
        }

        static JobTracker forCompaction(Collection<TableProperties> tables, CompactionJobTracker tracker) {
            return () -> WaitForJobsStatus.streamCompactionJobs(tracker, tables);
        }
    }

    @FunctionalInterface
    public interface TaskTracker {
        boolean hasRunningTasks();

        static TaskTracker forIngest(IngestTaskTracker tracker) {
            return () -> !tracker.getTasksInProgress().isEmpty();
        }

        static TaskTracker forCompaction(CompactionTaskTracker tracker) {
            return () -> !tracker.getTasksInProgress().isEmpty();
        }
    }
}
