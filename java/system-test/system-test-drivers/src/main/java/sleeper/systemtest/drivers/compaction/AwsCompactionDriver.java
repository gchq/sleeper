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

package sleeper.systemtest.drivers.compaction;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.sqs.AmazonSQS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.deploy.InvokeLambda;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.creation.CreateCompactionJobs;
import sleeper.compaction.job.creation.CreateCompactionJobs.Mode;
import sleeper.compaction.job.creation.SendCompactionJobToSqs;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.table.InvokeForTableRequest;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.compaction.CompactionDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

public class AwsCompactionDriver implements CompactionDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsCompactionDriver.class);

    private final SystemTestInstanceContext instance;
    private final LambdaClient lambdaClient;
    private final AmazonDynamoDB dynamoDBClient;
    private final AmazonSQS sqsClient;

    public AwsCompactionDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.lambdaClient = clients.getLambda();
        this.dynamoDBClient = clients.getDynamoDB();
        this.sqsClient = clients.getSqs();
    }

    @Override
    public List<String> createJobsGetIds() {
        CompactionJobStatusStore store = CompactionJobStatusStoreFactory
                .getStatusStoreWithStronglyConsistentReads(dynamoDBClient, instance.getInstanceProperties());
        Set<String> jobsBefore = allJobIds(store).collect(Collectors.toSet());
        InvokeLambda.invokeWith(lambdaClient, instance.getInstanceProperties().get(COMPACTION_JOB_CREATION_LAMBDA_FUNCTION));
        List<String> newJobs = allJobIds(store)
                .filter(not(jobsBefore::contains))
                .collect(Collectors.toUnmodifiableList());
        LOGGER.info("Created {} new compaction jobs", newJobs.size());
        return newJobs;
    }

    @Override
    public List<String> forceCreateJobsGetIds() {
        CompactionJobStatusStore store = CompactionJobStatusStoreFactory
                .getStatusStoreWithStronglyConsistentReads(dynamoDBClient, instance.getInstanceProperties());
        Set<String> jobsBefore = allJobIds(store).collect(Collectors.toSet());
        CreateCompactionJobs createJobs = new CreateCompactionJobs(
                ObjectFactory.noUserJars(), instance.getInstanceProperties(),
                instance.getTablePropertiesProvider(), instance.getStateStoreProvider(),
                new SendCompactionJobToSqs(instance.getInstanceProperties(), sqsClient)::send, store,
                Mode.FORCE_ALL_FILES_AFTER_STRATEGY);
        int batchSize = instance.getInstanceProperties().getInt(COMPACTION_JOB_CREATION_BATCH_SIZE);
        InvokeForTableRequest.forTables(
                instance.streamTableProperties().map(TableProperties::getStatus),
                batchSize, createJobs::createJobs);
        List<String> newJobs = allJobIds(store)
                .filter(not(jobsBefore::contains))
                .collect(Collectors.toUnmodifiableList());
        LOGGER.info("Created {} new compaction jobs", newJobs.size());
        return newJobs;
    }

    @Override
    public void invokeTasks(int expectedTasks, PollWithRetries poll) {
        CompactionTaskStatusStore store = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDBClient, instance.getInstanceProperties());
        long tasksFinishedBefore = store.getAllTasks().stream().filter(CompactionTaskStatus::isFinished).count();
        try {
            poll.pollUntil("tasks are started", () -> {
                InvokeLambda.invokeWith(lambdaClient, instance.getInstanceProperties().get(COMPACTION_TASK_CREATION_LAMBDA_FUNCTION));
                long tasksStarted = store.getAllTasks().size() - tasksFinishedBefore;
                LOGGER.info("Found {} running compaction tasks", tasksStarted);
                return tasksStarted >= expectedTasks;
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private Stream<String> allJobIds(CompactionJobStatusStore store) {
        return instance.streamTableProperties()
                .map(properties -> properties.get(TABLE_ID))
                .parallel()
                .flatMap(tableId -> store.streamAllJobs(tableId)
                        .map(CompactionJobStatus::getJobId));
    }
}
