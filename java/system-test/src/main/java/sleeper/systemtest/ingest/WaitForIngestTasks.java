/*
 * Copyright 2022 Crown Copyright
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
package sleeper.systemtest.ingest;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.ingest.status.store.task.DynamoDBIngestTaskStatusStore;
import sleeper.ingest.task.IngestTaskStatus;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.systemtest.SystemTestProperties;
import sleeper.systemtest.util.PollWithRetries;

import java.io.IOException;
import java.util.List;

public class WaitForIngestTasks {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForIngestTasks.class);
    private static final long POLL_INTERVAL_MILLIS = 30000;
    private static final int MAX_POLLS = 30;

    private final IngestTaskStatusStore statusStore;
    private final PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(POLL_INTERVAL_MILLIS, MAX_POLLS);

    public WaitForIngestTasks(
            IngestTaskStatusStore statusStore) {
        this.statusStore = statusStore;
    }

    public void pollUntilFinished() throws InterruptedException {
        poll.pollUntil("ingest tasks finished", this::isIngestTasksFinished);
    }

    private boolean isIngestTasksFinished() {
        List<IngestTaskStatus> tasks = statusStore.getTasksInProgress();
        LOGGER.info("Ingest task statuses: {}", tasks);
        return tasks.isEmpty();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2 || args.length > 3) {
            System.out.println("Usage: <instance id>");
            System.out.println("Status format can be status or full, defaults to status.");
            return;
        }

        String instanceId = args[0];

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();

        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.loadFromS3GivenInstanceId(s3Client, instanceId);
        IngestTaskStatusStore ingestTaskStatusStore = DynamoDBIngestTaskStatusStore.from(dynamoDBClient, systemTestProperties);

        WaitForIngestTasks wait = new WaitForIngestTasks(ingestTaskStatusStore);
        wait.pollUntilFinished();
        s3Client.shutdown();
        dynamoDBClient.shutdown();
    }
}
