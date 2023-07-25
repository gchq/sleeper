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
import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.deploy.InvokeLambda;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.ingest.status.store.task.IngestTaskStatusStoreFactory;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.INGEST_LAMBDA_FUNCTION;

public class IngestByQueueDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestByQueueDriver.class);

    private final InstanceProperties properties;
    private final IngestTaskStatusStore taskStatusStore;
    private final IngestJobStatusStore jobStatusStore;
    private final LambdaClient lambdaClient;
    private final PollWithRetries pollUntilTasksStarted = PollWithRetries
            .intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(5));

    public IngestByQueueDriver(SleeperInstanceContext instance,
                               AmazonDynamoDB dynamoDBClient, LambdaClient lambdaClient) {
        this(instance.getInstanceProperties(),
                IngestTaskStatusStoreFactory.getStatusStore(dynamoDBClient, instance.getInstanceProperties()),
                IngestJobStatusStoreFactory.getStatusStore(dynamoDBClient, instance.getInstanceProperties()),
                lambdaClient);
    }

    public IngestByQueueDriver(InstanceProperties properties,
                               IngestTaskStatusStore taskStatusStore,
                               IngestJobStatusStore jobStatusStore,
                               LambdaClient lambdaClient) {
        this.properties = properties;
        this.taskStatusStore = taskStatusStore;
        this.jobStatusStore = jobStatusStore;
        this.lambdaClient = lambdaClient;
    }

    public void invokeStandardIngestTasks() throws InterruptedException {
        int tasksFinishedBefore = taskStatusStore.getAllTasks().size() - taskStatusStore.getTasksInProgress().size();
        pollUntilTasksStarted.pollUntil("tasks are started", () -> {
            InvokeLambda.invokeWith(lambdaClient, properties.get(INGEST_LAMBDA_FUNCTION));
            return taskStatusStore.getAllTasks().size() > tasksFinishedBefore;
        });
    }

    public void waitForJobs(Collection<String> jobIds, PollWithRetries pollUntilJobsFinished)
            throws InterruptedException {
        LOGGER.info("Waiting for jobs to finish: {}", jobIds.size());
        pollUntilJobsFinished.pollUntil("jobs are finished", () -> {
            List<String> unfinishedJobIds = jobIds.stream()
                    .filter(this::isUnfinished)
                    .collect(Collectors.toUnmodifiableList());
            LOGGER.info("Unfinished jobs: {}", unfinishedJobIds.size());
            return unfinishedJobIds.size() == 0;
        });
    }

    private boolean isUnfinished(String jobId) {
        return jobStatusStore.getJob(jobId)
                .map(job -> !job.isFinished())
                .orElse(true);
    }
}
