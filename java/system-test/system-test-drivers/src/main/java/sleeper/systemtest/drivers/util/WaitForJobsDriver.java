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
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.time.Duration;
import java.util.Collection;
import java.util.function.Function;

public class WaitForJobsDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForJobsDriver.class);

    private final SleeperInstanceContext instance;
    private final Function<InstanceProperties, JobStatusStore> getStore;

    private WaitForJobsDriver(SleeperInstanceContext instance,
                              Function<InstanceProperties, JobStatusStore> getStore) {
        this.instance = instance;
        this.getStore = getStore;
    }

    public static WaitForJobsDriver forIngest(SleeperInstanceContext instance, AmazonDynamoDB dynamoDBClient) {
        return new WaitForJobsDriver(instance, properties -> ingestStore(dynamoDBClient, properties));
    }

    public static WaitForJobsDriver forCompaction(SleeperInstanceContext instance, AmazonDynamoDB dynamoDBClient) {
        return new WaitForJobsDriver(instance, properties -> compactionStore(dynamoDBClient, properties));
    }

    private static JobStatusStore ingestStore(AmazonDynamoDB dynamoDBClient, InstanceProperties properties) {
        IngestJobStatusStore store = IngestJobStatusStoreFactory.getStatusStore(dynamoDBClient, properties);
        return jobId -> WaitForJobsStatus.forIngest(store, jobId);
    }

    private static JobStatusStore compactionStore(AmazonDynamoDB dynamoDBClient, InstanceProperties properties) {
        CompactionJobStatusStore store = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, properties);
        return jobId -> WaitForJobsStatus.forCompaction(store, jobId);
    }

    public void waitForJobs(Collection<String> jobIds) throws InterruptedException {
        waitForJobs(jobIds, PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(10)));
    }

    public void waitForJobs(Collection<String> jobIds, PollWithRetries pollUntilJobsFinished)
            throws InterruptedException {
        JobStatusStore store = getStore.apply(instance.getInstanceProperties());
        LOGGER.info("Waiting for jobs to finish: {}", jobIds.size());
        pollUntilJobsFinished.pollUntil("jobs are finished", () -> {
            WaitForJobsStatus status = store.getStatus(jobIds);
            LOGGER.info("Jobs status: {}", status);
            return status.isAllFinished();
        });
    }

    @FunctionalInterface
    private interface JobStatusStore {
        WaitForJobsStatus getStatus(Collection<String> jobIds);
    }
}
