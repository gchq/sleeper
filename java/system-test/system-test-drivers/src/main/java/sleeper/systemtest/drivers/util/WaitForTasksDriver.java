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

package sleeper.systemtest.drivers.util;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.util.PollWithRetries;
import sleeper.ingest.status.store.task.DynamoDBIngestTaskStatusStore;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

public class WaitForTasksDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForTasksDriver.class);
    private final IngestTaskStatusStore taskStatusStore;

    public WaitForTasksDriver(SleeperInstanceContext instance, AmazonDynamoDB dynamoDB) {
        this.taskStatusStore = new DynamoDBIngestTaskStatusStore(dynamoDB, instance.getInstanceProperties());
    }

    public static WaitForTasksDriver from(SleeperInstanceContext instance, AmazonDynamoDB dynamoDB) {
        return new WaitForTasksDriver(instance, dynamoDB);
    }

    public void waitForTasksToStart(PollWithRetries pollUntilTasksStart) throws InterruptedException {
        pollUntilTasksStart.pollUntil("tasks have started", () -> {
            LOGGER.info("Checking if a task has been started");
            return !taskStatusStore.getTasksInProgress().isEmpty();
        });
    }
}
