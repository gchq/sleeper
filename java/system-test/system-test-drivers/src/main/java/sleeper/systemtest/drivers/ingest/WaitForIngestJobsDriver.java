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

package sleeper.systemtest.drivers.ingest;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.util.PollWithRetries;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class WaitForIngestJobsDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForIngestJobsDriver.class);

    private final SleeperInstanceContext instance;
    private final AmazonDynamoDB dynamoDBClient;

    public WaitForIngestJobsDriver(SleeperInstanceContext instance, AmazonDynamoDB dynamoDBClient) {
        this.instance = instance;
        this.dynamoDBClient = dynamoDBClient;
    }

    public void waitForJobs(Collection<String> jobIds, PollWithRetries pollUntilJobsFinished)
            throws InterruptedException {
        IngestJobStatusStore store = IngestJobStatusStoreFactory.getStatusStore(dynamoDBClient, instance.getInstanceProperties());
        LOGGER.info("Waiting for jobs to finish: {}", jobIds.size());
        pollUntilJobsFinished.pollUntil("jobs are finished", () -> {
            List<String> unfinishedJobIds = getUnfinishedJobIds(store, jobIds);
            LOGGER.info("Unfinished jobs: {}", unfinishedJobIds.size());
            return unfinishedJobIds.isEmpty();
        });
    }

    private List<String> getUnfinishedJobIds(IngestJobStatusStore store, Collection<String> jobIds) {
        return jobIds.stream()
                .filter(jobId -> isUnfinished(store, jobId))
                .collect(Collectors.toUnmodifiableList());
    }

    private boolean isUnfinished(IngestJobStatusStore store, String jobId) {
        return store.getJob(jobId)
                .map(job -> !job.isFinished())
                .orElse(true);
    }
}
