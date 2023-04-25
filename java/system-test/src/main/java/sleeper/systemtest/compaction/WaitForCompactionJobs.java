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
package sleeper.systemtest.compaction;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.util.PollWithRetries;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.systemtest.SystemTestProperties;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class WaitForCompactionJobs {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForCompactionJobs.class);
    private static final long POLL_INTERVAL_MILLIS = 30000;
    private static final int MAX_POLLS = 120;

    private final CompactionJobStatusStore statusStore;
    private final String tableName;
    private final PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(POLL_INTERVAL_MILLIS, MAX_POLLS);

    public WaitForCompactionJobs(
            CompactionJobStatusStore statusStore,
            String tableName) {
        this.statusStore = statusStore;
        this.tableName = tableName;
    }

    public void pollUntilFinished() throws InterruptedException {
        poll.pollUntil("compaction jobs finished", this::isCompactionJobsFinished);
    }

    private boolean isCompactionJobsFinished() {
        List<CompactionJobStatus> jobs = statusStore.getUnfinishedJobs(tableName);
        LOGGER.info("Compaction job statuses: {}", jobs.stream().map(CompactionJobStatusJson::new).collect(Collectors.toList()));
        return jobs.isEmpty();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 2) {
            System.out.println("Usage: <instance id> <table name>");
            return;
        }

        String instanceId = args[0];
        String tableName = args[1];

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();

        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.loadFromS3GivenInstanceId(s3Client, instanceId);
        CompactionJobStatusStore store = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, systemTestProperties);

        WaitForCompactionJobs wait = new WaitForCompactionJobs(store, tableName);
        wait.pollUntilFinished();
        s3Client.shutdown();
        dynamoDBClient.shutdown();
    }
}
