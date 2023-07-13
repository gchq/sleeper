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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.SystemTestProperties;
import sleeper.systemtest.util.InvokeSystemTestLambda;

import java.io.IOException;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.systemtest.util.InvokeSystemTestLambda.createSystemTestLambdaClient;

public class InvokeCompactionTaskCreationUntilAllJobsStarted {
    static final int POLL_INTERVAL_MILLIS = 10000;
    static final int MAX_POLLS = 5;
    private final PollWithRetries pollWithRetries;
    private final String tableName;
    private final CompactionJobStatusStore statusStore;
    private final InvokeSystemTestLambda.Client lambdaClient;
    private final SystemDefinedInstanceProperty lambdaProperty;

    private InvokeCompactionTaskCreationUntilAllJobsStarted(
            String tableName, CompactionJobStatusStore statusStore, InvokeSystemTestLambda.Client lambdaClient,
            SystemDefinedInstanceProperty lambdaProperty, PollWithRetries pollWithRetries) {
        this.tableName = tableName;
        this.statusStore = statusStore;
        this.lambdaClient = lambdaClient;
        this.lambdaProperty = lambdaProperty;
        this.pollWithRetries = pollWithRetries;
    }

    public static InvokeCompactionTaskCreationUntilAllJobsStarted forCompaction(
            String tableName, InstanceProperties properties, CompactionJobStatusStore statusStore) {
        return new InvokeCompactionTaskCreationUntilAllJobsStarted(tableName, statusStore,
                InvokeSystemTestLambda.client(createSystemTestLambdaClient(), properties),
                COMPACTION_TASK_CREATION_LAMBDA_FUNCTION,
                PollWithRetries.intervalAndMaxPolls(POLL_INTERVAL_MILLIS, MAX_POLLS));
    }

    public static InvokeCompactionTaskCreationUntilAllJobsStarted forSplitting(
            String tableName, CompactionJobStatusStore statusStore, InvokeSystemTestLambda.Client lambdaClient, PollWithRetries pollWithRetries) {
        return new InvokeCompactionTaskCreationUntilAllJobsStarted(tableName, statusStore, lambdaClient,
                SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION,
                pollWithRetries);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 1) {
            System.out.println("Usage: <instance id>");
            return;
        }

        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.loadFromS3GivenInstanceId(AmazonS3ClientBuilder.defaultClient(), args[0]);
        CompactionJobStatusStore statusStore = CompactionJobStatusStoreFactory.getStatusStore(
                AmazonDynamoDBClientBuilder.defaultClient(), systemTestProperties);

        forCompaction("system-test", systemTestProperties, statusStore).pollUntilFinished();
    }

    public void pollUntilFinished() throws InterruptedException {
        pollWithRetries.pollUntil("all compaction jobs have started", () -> {
            lambdaClient.invokeLambda(lambdaProperty);
            return statusStore.getAllJobs(tableName).stream().allMatch(CompactionJobStatus::isStarted);
        });
    }
}
