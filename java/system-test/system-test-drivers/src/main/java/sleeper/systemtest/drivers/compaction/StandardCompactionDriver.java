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

package sleeper.systemtest.drivers.compaction;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.deploy.InvokeLambda;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;

public class StandardCompactionDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardCompactionDriver.class);

    private final SleeperInstanceContext instance;
    private final LambdaClient lambdaClient;
    private final AmazonDynamoDB dynamoDBClient;

    public StandardCompactionDriver(SleeperInstanceContext instance,
                                    LambdaClient lambdaClient, AmazonDynamoDB dynamoDBClient) {
        this.instance = instance;
        this.lambdaClient = lambdaClient;
        this.dynamoDBClient = dynamoDBClient;
    }

    public List<String> createJobsGetIds() {
        CompactionJobStatusStore store = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, instance.getInstanceProperties());
        Set<String> jobsBefore = allJobIds(store).collect(Collectors.toSet());
        InvokeLambda.invokeWith(lambdaClient, instance.getInstanceProperties().get(COMPACTION_JOB_CREATION_LAMBDA_FUNCTION));
        return allJobIds(store)
                .filter(not(jobsBefore::contains))
                .collect(Collectors.toUnmodifiableList());
    }

    public void invokeTasks(int expectedTasks, PollWithRetries poll) throws InterruptedException {
        CompactionTaskStatusStore store = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDBClient, instance.getInstanceProperties());
        int tasksFinishedBefore = store.getAllTasks().size() - store.getTasksInProgress().size();
        poll.pollUntil("tasks are started", () -> {
            InvokeLambda.invokeWith(lambdaClient, instance.getInstanceProperties().get(COMPACTION_TASK_CREATION_LAMBDA_FUNCTION));
            int tasksStarted = store.getAllTasks().size() - tasksFinishedBefore;
            LOGGER.info("Found {} new compaction tasks", tasksStarted);
            return tasksStarted >= expectedTasks;
        });
    }

    private Stream<String> allJobIds(CompactionJobStatusStore store) {
        return store.getAllJobs(instance.getTableName()).stream().map(CompactionJobStatus::getJobId);
    }
}
