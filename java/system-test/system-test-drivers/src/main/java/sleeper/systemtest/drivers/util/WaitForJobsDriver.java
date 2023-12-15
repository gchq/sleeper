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

package sleeper.systemtest.drivers.util;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.ingest.status.store.task.IngestTaskStatusStoreFactory;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.function.Function;

public class WaitForJobsDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForJobsDriver.class);

    private final SleeperInstanceContext instance;
    private final String typeDescription;
    private final Function<InstanceProperties, JobStatusStore> getJobsStore;
    private final Function<InstanceProperties, TaskStatusStore> getTasksStore;

    private WaitForJobsDriver(SleeperInstanceContext instance,
                              String typeDescription,
                              Function<InstanceProperties, JobStatusStore> getJobsStore,
                              Function<InstanceProperties, TaskStatusStore> getTasksStore) {
        this.instance = instance;
        this.typeDescription = typeDescription;
        this.getJobsStore = getJobsStore;
        this.getTasksStore = getTasksStore;
    }

    public static WaitForJobsDriver forIngest(SleeperInstanceContext instance, AmazonDynamoDB dynamoDBClient) {
        return new WaitForJobsDriver(instance, "ingest",
                properties -> ingestJobsStore(dynamoDBClient, properties),
                properties -> ingestTasksStore(dynamoDBClient, properties));
    }

    public static WaitForJobsDriver forCompaction(SleeperInstanceContext instance, AmazonDynamoDB dynamoDBClient) {
        return new WaitForJobsDriver(instance, "compaction",
                properties -> compactionJobsStore(dynamoDBClient, properties),
                properties -> compactionTasksStore(dynamoDBClient, properties));
    }

    private static JobStatusStore ingestJobsStore(AmazonDynamoDB dynamoDBClient, InstanceProperties properties) {
        IngestJobStatusStore store = IngestJobStatusStoreFactory.getStatusStore(dynamoDBClient, properties);
        return jobId -> WaitForJobsStatus.forIngest(store, jobId, Instant.now());
    }

    private static TaskStatusStore ingestTasksStore(AmazonDynamoDB dynamoDBClient, InstanceProperties properties) {
        IngestTaskStatusStore store = IngestTaskStatusStoreFactory.getStatusStore(dynamoDBClient, properties);
        return () -> store.getTasksInProgress().size();
    }

    private static JobStatusStore compactionJobsStore(AmazonDynamoDB dynamoDBClient, InstanceProperties properties) {
        CompactionJobStatusStore store = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, properties);
        return jobId -> WaitForJobsStatus.forCompaction(store, jobId, Instant.now());
    }

    private static TaskStatusStore compactionTasksStore(AmazonDynamoDB dynamoDBClient, InstanceProperties properties) {
        CompactionTaskStatusStore store = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDBClient, properties);
        return () -> store.getTasksInProgress().size();
    }

    public void waitForJobs(Collection<String> jobIds) throws InterruptedException {
        waitForJobs(jobIds, PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(1), Duration.ofMinutes(10)));
    }

    public void waitForJobs(Collection<String> jobIds, PollWithRetries pollUntilJobsFinished)
            throws InterruptedException {
        InstanceProperties properties = instance.getInstanceProperties();
        JobStatusStore store = getJobsStore.apply(properties);
        TaskStatusStore tasksStore = getTasksStore.apply(properties);
        LOGGER.info("Waiting for {} jobs to finish: {}", typeDescription, jobIds.size());
        pollUntilJobsFinished.pollUntil("jobs are finished", () -> {
            WaitForJobsStatus status = store.getStatus(jobIds);
            LOGGER.info("Status of {} jobs: {}", typeDescription, status);
            if (status.isAllFinished()) {
                return true;
            }
            if (tasksStore.countRunningTasks() < 1) {
                throw new IllegalStateException("Found no tasks running while waiting for " + typeDescription + " jobs");
            }
            return false;
        });
    }

    @FunctionalInterface
    private interface JobStatusStore {
        WaitForJobsStatus getStatus(Collection<String> jobIds);
    }

    @FunctionalInterface
    private interface TaskStatusStore {
        int countRunningTasks();
    }
}
